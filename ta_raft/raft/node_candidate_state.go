package raft

// __BEGIN_TA__
import "time"

// __END_TA__

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *RaftNode) doCandidate() stateFunction {
	r.Out("Transitioning to CANDIDATE_STATE")
	r.State = CANDIDATE_STATE
	// __BEGIN_TA__
	// Create channel to read election results
	electionResults := make(chan bool)
	shouldFallback := make(chan bool)
	var nextTimeout <-chan time.Time

	// Update current term
	currTerm := r.GetCurrentTerm() + 1
	r.setVotedFor("")
	r.setCurrentTerm(currTerm)

	// Request votes from other nodes
	go r.requestVotes(electionResults, shouldFallback, currTerm)

	// Begin candidate logic...
	for {
		r.setVotedFor(r.GetRemoteSelf().Id)

		select {
		case result := <-electionResults:
			r.Out("Received election result (%v)", result)
			if result {
				// We won the election. Become the leader.
				return r.doLeader
			}

			// We didn't win the election. Wait for a bit, and then try again,
			// to give a better-qualified candidate a chance to win if it can
			nextTimeout = randomTimeout(r.config.ElectionTimeout)

		case <-nextTimeout:
			// We've timed out. Disable this timeout and begin a new election.
			// Satisfies candidate rule #4 from "rules for servers" in figure 2
			r.Out("Timed out; beginning another election")

			// Update term again
			currTerm := r.GetCurrentTerm() + 1
			r.setVotedFor("")
			r.setCurrentTerm(currTerm)

			// Request votes
			go r.requestVotes(electionResults, shouldFallback, currTerm)
			nextTimeout = nil

		case <-shouldFallback:
			return r.doFollower

		case msg := <-r.appendEntries:
			r.Out("Received append entries request in CANDIDATE state")
			if _, fallback := r.handleAppendEntries(msg); fallback {
				return r.doFollower
			}

		case msg := <-r.requestVote:
			r.Out("Received vote request")
			if r.handleCompetingRequestVote(msg) {
				return r.doFollower
			}

		case msg := <-r.registerClient:
			reply := msg.reply
			// There is currently no leader since we are in the middle of an election,
			// thus we cannot register a new client session.
			reply <- RegisterClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				ClientId:   0,
				LeaderHint: r.GetRemoteSelf(),
			}

		case msg := <-r.clientRequest:
			reply := msg.reply
			// There is currently no leader since we are in the middle of an election,
			// thus we cannot register a new client session.
			reply <- ClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				Response:   "nil",
				LeaderHint: r.GetRemoteSelf(),
			}

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
	// candidate state should do when it receives an incoming message on every
	// possible channel.
	return nil
	__END_STUDENT__ */
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (r *RaftNode) requestVotes(electionResults chan bool, fallback chan bool, currTerm uint64) {
	// __BEGIN_TA__
	// Satisfies candidate duty #1 (and sub-parts) from "rules for servers" in figure 2
	lastLogIndex := r.getLastLogIndex()
	lastEntry := r.getLogEntry(lastLogIndex)
	lastLogTerm := lastEntry.TermId

	grantedCount := 1
	quorum := (r.config.ClusterSize / 2) + 1
	request := RequestVoteRequest{
		Term:         currTerm,
		Candidate:    r.GetRemoteSelf(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// For each node in the cluster that isn't us:
	// - Request the vote
	// - Make sure we're in the right term
	// - Increase vote count if we win
	for _, node := range r.GetNodeList() {
		if node.Id == r.GetRemoteSelf().Id {
			continue
		}

		reply, err := (&node).RequestVoteRPC(r, &request)

		if err != nil {
			r.Error("Failed to request a vote from %v (%v)\n", node, err)
			continue
		}

		if reply.Term > currTerm {
			// We're in a previous term, so forfeit the election!
			r.setVotedFor("")
			r.setCurrentTerm(reply.Term)
			fallback <- true
			return
		}

		if reply.VoteGranted {
			grantedCount++
		}
	}

	// Satisfies candidate duty #2 from "rules for servers" in figure 2
	electionResults <- grantedCount >= quorum
	// __END_TA__
	/* __BEGIN_STUDENT__
	// TODO: Students should implement this method
	return
	__END_STUDENT__ */
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *RaftNode) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {
	// __BEGIN_TA__
	request := msg.request
	reply := msg.reply

	if request.Term > r.GetCurrentTerm() {
		// Received a request from a candidate in a future term. Fallback.
		// Partially satisfies rule part 2 in "All Servers" section of "Rules for Servers".
		r.Out("Received vote request with newer term than ours, updating term from %v -> %v\n", r.GetCurrentTerm(), request.Term)
		r.setVotedFor("")
		r.setCurrentTerm(request.Term)
		fallback = true

		rep := RequestVoteReply{r.GetCurrentTerm(), false}

		localLastLogIndex := r.getLastLogIndex()
		localLastLogTerm := r.getLogEntry(localLastLogIndex).GetTermId()

		// vote iff. last log term is greater, or last log term is equal to local and
		// last log index is greater or equal to local
		if request.LastLogTerm != localLastLogTerm {
			rep.VoteGranted = request.LastLogTerm > localLastLogTerm
		} else {
			rep.VoteGranted = request.LastLogIndex >= localLastLogIndex
		}

		if rep.VoteGranted {
			r.setVotedFor(request.Candidate.Id)
		}

		reply <- rep
		return
	} else {
		r.Out("Received competing vote request; not granting")

		reply <- RequestVoteReply{
			r.GetCurrentTerm(),
			false,
		}

		return false
	}
	// __END_TA__
	/* __BEGIN_STUDENT__
	// TODO: Students should implement this method
	return true
	__END_STUDENT__ */
}
