package raft

import (
	"sync"
	"time"
)

// doFollower implements the logic for a Raft node in the follower state.
func (r *RaftNode) doFollower() stateFunction {

	r.initFollowerState()

	// election timer for handling going into candidate state
	electionTimer := r.randomTimeout(r.config.ElectionTimeout)

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				r.Out("Shutting down")
				return nil
			}

		case <-electionTimer:
			// if we timeout with no appendEntries heartbeats,
			// start election with this node
			r.Out("Election timeout")

			// for debugging purposes:
			if r.debugCond != nil {
				r.debugCond.L.Lock()
				r.Out("Waiting for broadcast...")
				r.debugCond.Wait()
				r.debugCond.L.Unlock()
			}

			return r.doCandidate

		case msg := <-r.requestVote:
			if votedFor, _ := r.handleRequestVote(msg); votedFor {
				// reset timeout if voted so not all (non-candidate-worthy) nodes become candidates at once
				r.Debug("Election timeout reset")
				electionTimer = r.randomTimeout(r.config.ElectionTimeout)
			}
		case msg := <-r.appendEntries:
			if resetTimeout, _ := r.handleAppendEntries(msg); resetTimeout {
				electionTimer = r.randomTimeout(r.config.ElectionTimeout)
			}
		case msg := <-r.registerClient:
			r.Out("RegisterClient received")
			r.handleRegisterClientAsNonLeader(msg)

		case msg := <-r.clientRequest:
			r.handleClientRequestAsNonLeader(msg)
		}
	}
}

func (r *RaftNode) initFollowerState() {
	r.requestsMutex.Lock()
	defer r.requestsMutex.Unlock()

	r.State = FOLLOWER_STATE
	r.Out("Transitioning to FOLLOWER_STATE")
	for cacheID, replyChan := range r.requestsByCacheId {
		if r.Leader == nil {
			replyChan <- ClientReply{Status: ClientStatus_ELECTION_IN_PROGRESS, LeaderHint: r.GetRemoteSelf()}
		} else {
			replyChan <- ClientReply{Status: ClientStatus_NOT_LEADER, LeaderHint: r.Leader}
		}
		delete(r.requestsByCacheId, cacheID)
	}
}

// STUDENT WRITTEN
// shouldVoteFor returns true if node r should cast its vote and
// false otherwise.
func (r *RaftNode) shouldVoteFor(req *RequestVoteRequest) bool {
	// Candidate must have equal or higher term for a chance
	if req.GetTerm() < r.GetCurrentTerm() {
		r.Out("Rejected to vote for %v because of lower term", req.Candidate.Id)
		return false
	}

	// Candidate's log is not less up to date
	if r.logMoreUpdatedThan(req.GetLastLogIndex(), req.GetLastLogTerm()) {
		return false
	}

	// The node has already voted for another candidate in this term
	if req.GetTerm() == r.GetCurrentTerm() && r.GetVotedFor() != "" && r.GetVotedFor() != req.GetCandidate().GetId() {
		r.Out("Rejected %v because already voted in term", req.Candidate.Id)
		return false
	}

	return true
}

// STUDENT WRITTEN
// Helper to check for a required term update and perform necessary operations
// to update our term if necessary. Should be called or used WHENEVER communicating
// with another node, which may involve falling back.
// Returns whether our term was updated (i.e. whether a candidate or leader should fall back)
func (r *RaftNode) updateTermIfNecessary(otherNodeTerm uint64) (updated bool) {
	if otherNodeTerm > r.GetCurrentTerm() {
		ok := r.setCurrentTerm(otherNodeTerm)
		if ok {
			r.setVotedFor("")
		}
		return ok
	}
	return false
}

// handleRequestVote handles a vote request and updates itself it necessary.
// It will consider the "campaign" of the candidate relative to it's own
// list of log entries to determine if that node is a better candidate than it.
func (r *RaftNode) handleRequestVote(msg RequestVoteMsg) (votingFor bool, fallback bool) {
	votingFor = r.shouldVoteFor(msg.request)
	higherTerm := r.updateTermIfNecessary(msg.request.GetTerm())
	if votingFor {
		r.setVotedFor(msg.request.GetCandidate().GetId())
	}
	Debug.Printf("Vote for %v: %v", msg.request.Candidate.Id, votingFor)
	msg.reply <- RequestVoteReply{r.GetCurrentTerm(), votingFor}

	// we should fall back (if we're a candidate) if we'd vote for this candidate OR they're a higher term
	return votingFor, votingFor || higherTerm
}

// handleRegisterClientAsNonLeader handles an incoming RegisterClientMsg. It is called by
// a client attempting to connect to this node. Since we're a follower, this
// request will be redirected to the leader by providing the client with the leader location.
func (r *RaftNode) handleRegisterClientAsNonLeader(msg RegisterClientMsg) {
	if r.Leader == nil {
		msg.reply <- RegisterClientReply{ClientStatus_ELECTION_IN_PROGRESS, 0, r.GetRemoteSelf()}
	} else {
		msg.reply <- RegisterClientReply{ClientStatus_NOT_LEADER, 0, r.Leader}
	}
}

// handleClientRequestAsNonLeader handles an incoming ClientRequestMsg. It is called by
// a client attempting to fulfill a request. Since we're a follower, this
// request will be redirected to the leader by providing the client with the leader location.
func (r *RaftNode) handleClientRequestAsNonLeader(msg ClientRequestMsg) {
	if r.Leader == nil {
		msg.reply <- ClientReply{ClientStatus_ELECTION_IN_PROGRESS, "", r.GetRemoteSelf()}
	} else {
		msg.reply <- ClientReply{ClientStatus_NOT_LEADER, "", r.Leader}
	}
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
// CASES:
// - if request has lower term, never fallback and do not resetTimeout
// - node follower with lower or equal term -- fallback = true and resetTimeout = true, regardless of whether the
// request was successful or not. The fact that the leader node is making an effort is enough.
// - if a candidate and get a request from a node with a higher term, fallback = resetTimeout = true
// - if another leader, only fallback if terms. resetTimeout is irrelevant (this can happen with a network partitionN)
func (r *RaftNode) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	if len(msg.request.GetEntries()) > 0 {
		r.Debug("Got appendEntries with %d entries from %v", len(msg.request.GetEntries()), msg.request.GetLeader())
	} else {
		r.Verbose("Got appendEntries heartbeat from %v", msg.request.GetLeader().Id)
	}

	// resetTimeout == request successful
	if msg.request.GetTerm() < r.GetCurrentTerm() {
		// if the leader calling us is behind the times the request is unsuccessful, and it should revert
		msg.reply <- AppendEntriesReply{r.GetCurrentTerm(), false} // our term is greater the leader's
		return false, false

	} else {
		// node has higher or equivalent term and so this is an acceptable heartbeat
		// make sure we have this leader as our leader and the correct term
		r.updateTermIfNecessary(msg.request.GetTerm())

		// no matter our state, we'll always be reverting to a follower when getting an AppendEntries,
		// so set our leader to be the cluster leader (who will also be the one who sent the message)
		r.Leader = msg.request.GetLeader()

		success := r.mergeLogEntries(msg.request)
		msg.reply <- AppendEntriesReply{r.GetCurrentTerm(), success}

		// always "fall back", but this will only be utilized by leaders and candidates
		return true, true
	}
}

// Sends resetTimeout messages to reset
func (r *RaftNode) listenForHeartbeats(timeout *<-chan time.Time, exit chan struct{}, group *sync.WaitGroup) {
	defer group.Done()
	for {
		select {
		case msg := <-r.appendEntries:
			if resetTimeout, _ := r.handleAppendEntries(msg); resetTimeout {
				r.Debug("Timeout reset by heartbeat")
				*timeout = r.randomTimeout(r.config.ElectionTimeout)
			}
		case <-exit:
			return
		}
	}
}

// Given an AppendEntriesRequest, mergeLogEntries attempts to merge the log data into
// this node's log. Returns whether this was successful, i.e. whether leader should try again with
// earlier PrevLogEntry. If there are no entries to append, if the previous value was found, it
// should return true.
func (r *RaftNode) mergeLogEntries(req *AppendEntriesRequest) (success bool) {

	r.leaderMutex.Lock()
	defer r.leaderMutex.Unlock()

	entries := req.GetEntries()

	// if prevLogIndex is out of range, cannot merge
	if req.GetPrevLogIndex() < 0 || req.GetPrevLogIndex() > r.getLastLogIndex() {
		r.Out("MERGING: Couldn't find prev")
		return false
	}

	// if log doesn't contain entry at prevLogIndex with term PrevLogTerm, cannot merge
	followerPrevLog := r.getLogEntry(req.GetPrevLogIndex())
	if followerPrevLog.TermId != req.GetPrevLogTerm() {
		r.Out("MERGING: Couldn't find prevEntry with term = %d; index = %d", req.GetPrevLogTerm(), req.GetPrevLogIndex())
		return false
	}

	// if there are entries present, merge them
	if entries != nil && len(entries) != 0 {
		for i := range entries {
			// index of where we would insert the new item
			insertAt := uint64(i) + req.GetPrevLogIndex() + 1
			if entries[i].GetIndex() != insertAt {
				r.Error("Request doesn't have correct index!! State corrupted.")
			}
			r.Out("Merging logs: adding %v", entries[i])
			if insertAt <= r.getLastLogIndex() {
				r.truncateLog(insertAt)
				r.appendLogEntry(*entries[i])
			} else {
				// if we go past the end of the log (or remove entries above), keep appending
				r.appendLogEntry(*entries[i])
			}
		}
	}

	// apply all logEntries up until leader's commitIndex to statemachine
	if req.GetLeaderCommit() > r.commitIndex {
		newCommitIndex := min(req.GetLeaderCommit(), r.getLastLogIndex())
		// start at +1 since commitIndex has already been committed
		for i := r.commitIndex + 1; i <= newCommitIndex; i++ {
			entry := r.getLogEntry(i)
			r.Out("COMMITTING index=%v;term=%v", entry.GetIndex(), entry.GetTermId())
			if entry.Type == CommandType_STATE_MACHINE_COMMAND {
				response, err := r.stateMachine.ApplyCommand(entry.Command, entry.Data)
				if err != nil {
					r.Error("State machine error: %v (response: %s)", err, response)
				}
			}
			r.lastApplied = i
		}
		r.commitIndex = newCommitIndex
	}

	r.Verbose("Merge successful")
	return true
}
