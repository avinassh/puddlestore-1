package raft

// __BEGIN_TA__
import "math"

// __END_TA__

// doFollower implements the logic for a Raft node in the follower state.
func (r *RaftNode) doFollower() stateFunction {
	r.Out("Transitioning to FOLLOWER_STATE")
	r.State = FOLLOWER_STATE
	// __BEGIN_TA__

	// If there's anyone pending a response, we should inform them that
	// we are no longer the leader, and that they should retry on the
	// new leader.
	var notLeader ClientReply
	if r.Leader == nil {
		notLeader = ClientReply{
			Status:     ClientStatus_ELECTION_IN_PROGRESS,
			Response:   "nil",
			LeaderHint: r.GetRemoteSelf(),
		}
	} else {
		notLeader = ClientReply{
			Status:     ClientStatus_NOT_LEADER,
			Response:   "nil",
			LeaderHint: r.Leader,
		}
	}
	r.requestsMutex.Lock()
	for _, v := range r.requestsByCacheId {
		v <- notLeader
	}

	// Replace old maps, since their info is no longer useful
	r.requestsByCacheId = make(map[string]chan ClientReply)
	r.requestsMutex.Unlock()

	// Begin follower logic...
	timeout := randomTimeout(r.config.ElectionTimeout)
	for {
		select {
		case msg := <-r.appendEntries:
			resetTimeout, _ := r.handleAppendEntries(msg)
			if resetTimeout {
				timeout = randomTimeout(r.config.ElectionTimeout)
			}

		case msg := <-r.requestVote:
			request := msg.request
			reply := msg.reply

			if request.GetTerm() < r.GetCurrentTerm() {
				// return immediately if term is out of date
				reply <- RequestVoteReply{r.GetCurrentTerm(), false}
				continue
			}

			if request.GetTerm() > r.GetCurrentTerm() {
				// if it's a later term, reset term and voted for
				r.setCurrentTerm(request.Term)
				r.setVotedFor("")
			}

			if r.GetVotedFor() != "" && r.GetVotedFor() != request.GetCandidate().GetId() {
				// return if already voted in current term, and not for this candidate
				reply <- RequestVoteReply{r.GetCurrentTerm(), false}
				continue
			}

			// prepare to make voting decision
			var grantVote bool
			localLastLogIndex := r.getLastLogIndex()
			localLastLogTerm := r.getLogEntry(localLastLogIndex).GetTermId()

			// vote iff. last log term is greater, or last log term is equal to local
			// and last log index is greater or equal to local
			if request.LastLogTerm != localLastLogTerm {
				grantVote = request.LastLogTerm > localLastLogTerm
			} else {
				grantVote = request.LastLogIndex >= localLastLogIndex
			}

			// if we granted a vote, set votedFor and reset the timeout
			if grantVote {
				r.setVotedFor(request.Candidate.Id)
				timeout = randomTimeout(r.config.ElectionTimeout)
			}

			reply <- RequestVoteReply{r.GetCurrentTerm(), grantVote}

		case msg := <-r.registerClient:
			reply := msg.reply
			r.Debug("We are not the leader, cannot process client request\n")

			if r.Leader != nil {
				// We are not the leader, so we cannot register a client
				reply <- RegisterClientReply{
					Status:     ClientStatus_NOT_LEADER,
					ClientId:   0,
					LeaderHint: r.Leader,
				}
			} else {
				// For some reason we don't know who the leader is, I guess we should
				// just respond with our own address as a leader hint.
				reply <- RegisterClientReply{
					Status:     ClientStatus_NOT_LEADER,
					ClientId:   0,
					LeaderHint: r.GetRemoteSelf(),
				}
			}

		case msg := <-r.clientRequest:
			reply := msg.reply
			r.Debug("We are not the leader, cannot process client request\n")

			// We are not the leader, so we cannot process this request
			if r.Leader != nil {
				reply <- ClientReply{
					Status:     ClientStatus_NOT_LEADER,
					Response:   "nil",
					LeaderHint: r.Leader,
				}
			} else {
				// For some reason we don't know who the leader is, I guess we
				// should just respond with our own address as a leader hint.
				reply <- ClientReply{
					Status:     ClientStatus_NOT_LEADER,
					Response:   "nil",
					LeaderHint: r.GetRemoteSelf(),
				}
			}

		case <-timeout:
			r.Leader = nil
			return r.doCandidate

		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
	// __END_TA__

	/* __BEGIN_STUDENT__
	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
	__END_STUDENT__ */
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (r *RaftNode) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	/* __BEGIN_STUDENT__
	// TODO: Students should implement this method
	return
	__END_STUDENT__ */
	// __BEGIN_TA__
	request := msg.request
	reply := msg.reply

	// If leader term is lower than ours, reject
	if request.Term < r.GetCurrentTerm() {
		r.Out("Received AppendEntries request with lower term, rejecting")
		reply <- AppendEntriesReply{
			r.GetCurrentTerm(),
			false,
		}

		return false, false
	}

	// Fallback since request term is equal or higher
	fallback = true

	// Set leader to be whomever is sending us append entries requests
	if r.Leader == nil || (r.Leader.Id != request.Leader.Id) {
		oldLeaderId := "nil"
		if r.Leader != nil {
			oldLeaderId = r.Leader.Id
		}
		r.Leader = request.Leader
		r.Out("Updated leader from %v -> %v\n", oldLeaderId, request.Leader.Id)
	}

	// Update term and clear votedFor if request is from higher term
	if request.Term > r.GetCurrentTerm() {
		r.Out("Received AppendEntries with higher term than ours, updating from %v -> %v\n",
			r.GetCurrentTerm(), request.Term)
		r.setCurrentTerm(request.Term)
		r.setVotedFor("")
	}

	// Reset timeout since we've accepted the leader as valid
	resetTimeout = true

	if request.PrevLogIndex > r.getLastLogIndex() {
		// (Partial check of rule 2 of "AppendEntriesRPC" from figure 2 of Raft paper)
		//
		// If request PrevLogIndex is higher than our LastLogIndex, then the leader's
		// conception of our logs (specifically log length) is false, reject
		r.Out("Received message with previous log index greater than ours (req.prevLogIdx=%v, lastLogIdx=%v)", request.PrevLogIndex, r.getLastLogIndex())

		reply <- AppendEntriesReply{
			r.GetCurrentTerm(),
			false,
		}
	} else if entry := r.getLogEntry(request.PrevLogIndex); entry != nil && (entry.TermId != request.PrevLogTerm) {
		// (Remaining check of rule 2 of "AppendEntriesRPC" from figure 2 of Raft paper)
		//
		// If our log doesn’t contain an entry at PrevLogIndex whose term matches PrevLogTerm,
		// then leader's conception of our log is false, reject
		r.Out("Log entry already exists for PrevLogIndex")

		reply <- AppendEntriesReply{
			r.GetCurrentTerm(),
			false,
		}
	} else {
		// Accept request; truncate if necessary and append any new entries

		// Lock leaderMutex (as opposed to another mutex) since it controls access
		// to the logCache when in leader state, and we should use the same mutex here too.
		// TODO: add a logCacheMutex to the node struct in the future
		r.leaderMutex.Lock()

		if len(request.Entries) > 0 {
			r.Out("Successfully received %d entries", len(request.Entries))

			newFirstEntry := request.Entries[0]
			ourLastEntry := r.getLogEntry(r.getLastLogIndex())

			// Truncate log if necessary...
			if (ourLastEntry.Index >= newFirstEntry.Index) ||
				((ourLastEntry.Index == newFirstEntry.Index) && (ourLastEntry.TermId != newFirstEntry.TermId)) {
				// Our last entry conflicts with the first entry sent by the leader; time
				// to truncate our log from the lower of the two values (inclusive), all
				// the way to the end of the log.
				r.Out("Truncating log to remove index %v and beyond", newFirstEntry.Index)
				err := r.truncateLog(newFirstEntry.Index)
				if err != nil {
					// For some reason we couldn't truncate our log, this should never happen.
					panic(err)
				}
			}

			// At this point, our LastLogIndex + 1 == newFirstEntry.Index; safe to append new entries
			if r.getLastLogIndex()+1 != newFirstEntry.GetIndex() {
				panic("after truncation, local LastLogIndex + 1 is not equal to newFirstEntry.Index")
			}

			// Append new entries...
			for _, entry := range request.Entries {
				err := r.appendLogEntry(*entry)
				if err != nil {
					// For some reason we couldn't append a log entry, this should never happen.
					panic(err)
				}
			}
		}

		r.leaderMutex.Unlock()

		// Update commit index, process any newly committed log entries, update lastApplied
		if request.LeaderCommit > r.commitIndex {
			newCommitIdx := uint64(math.Min(float64(request.LeaderCommit), float64(r.getLastLogIndex())))
			r.Out("Updating commitIndex from %v -> %v", r.commitIndex, uint64(newCommitIdx))

			for newCommitIdx > r.lastApplied {
				r.lastApplied++
				r.processLogEntry(*r.getLogEntry(r.lastApplied))
			}

			r.commitIndex = newCommitIdx
		}

		// Reply with success!
		reply <- AppendEntriesReply{
			r.GetCurrentTerm(),
			true,
		}
	}

	return
	// __END_TA__
}
