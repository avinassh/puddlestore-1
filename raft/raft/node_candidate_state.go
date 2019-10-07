package raft

import (
	"sync"
	"time"
)

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *RaftNode) doCandidate() stateFunction {

	r.initCandidate()

	// election timer for handling into candidate state
	electionTimer := r.randomTimeout(r.config.ElectionTimeout)

	// send vote requests to all nodes, and then later wait on return channel for results
	win := make(chan struct{}, 1)
	fallback := make(chan struct{}, len(r.GetNodeList()))
	wg := sync.WaitGroup{}
	wg.Add(1)
	go r.requestVotes(win, fallback, &wg)
	cleanup := true

	defer func() {
		if cleanup {
			wg.Wait()
		}
	}()

	for {
		//r.Verbose("Waiting on event...")
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				r.Out("Shutting down")
				return nil
			}
		case <-electionTimer:
			r.Debug("Election timeout")
			// if we timeout with no appendEntries heartbeats, re-start election
			return r.doCandidate
		case <-win:
			r.Out("Election win received on channel!")
			cleanup = false
			return r.doLeader

		case <-fallback:
			r.Out("Fallback command received from voting")
			return r.doFollower

		case msg := <-r.appendEntries:
			if _, fallback := r.handleAppendEntries(msg); fallback {
				r.Out("Fallback received from appendEntries")
				return r.doFollower
			}
			// resetTimeout doesn't matter because we either completely
			// ignore an appendEntries or change state based on it
		case msg := <-r.requestVote:
			if _, fallback := r.handleRequestVote(msg); fallback {
				r.Out("Fallback received from requestVote")
				return r.doFollower
			}
		case msg := <-r.registerClient:
			r.handleRegisterClientAsNonLeader(msg)

		case msg := <-r.clientRequest:
			r.handleClientRequestAsNonLeader(msg)
		}
	}
}

func (r *RaftNode) initCandidate() {

	r.nodeMutex.Lock()
	defer r.nodeMutex.Unlock()

	r.Out("Transitioning to CANDIDATE_STATE; TIMESTAMP: %v", time.Now())
	r.State = CANDIDATE_STATE
	r.Leader = nil

	// set up for election:
	// increment term for new leader era (note we'll either be the new leader in this term or
	// someone else will, so it's always necessary to increment)
	r.setCurrentTerm(r.GetCurrentTerm() + 1)
	r.setVotedFor(r.GetRemoteSelf().GetAddr())
}

// Sends requestVote messages to all current nodes, sending struct{}{} to electionResults if winning.
// Sends nothing if did not win election. Sends fallback appropriately to the fallback channel
func (r *RaftNode) requestVotes(win, fallback chan struct{}, group *sync.WaitGroup) {

	defer group.Done()

	// compile our log data into the request, which is constant across all nodes
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := uint64(0) // default
	if lastLogIndex >= 0 {
		lastLogTerm = r.getLogEntry(lastLogIndex).TermId
	}
	request := RequestVoteRequest{r.GetCurrentTerm(), r.GetRemoteSelf(), lastLogIndex, lastLogTerm}
	// we start with one vote because we vote for ourselves
	votes := 1
	nodes := r.GetNodeList()
	wg := sync.WaitGroup{}
	success := make(chan bool, len(nodes)) //will receive at maximum len(nodes) -1 items on the success channel
	for i, node := range nodes {
		// don't send requestVotes to ourselves!
		if node.GetAddr() != r.GetRemoteSelf().GetAddr() {
			wg.Add(1)
			go r.requestVoteFromNode(&nodes[i], &request, success, fallback, &wg)
		}
	}

	// short-circuit if received majority
	for i := 0; i < len(nodes)-1; i++ {
		if vote := <-success; vote {
			votes++
		}
		if votes > (len(nodes) / 2) {
			r.Out("Won election!")
			win <- struct{}{}
			break
		}
	}

	// wg still necessary if short circuited without all goroutines cleaned
	wg.Wait()
	r.Out("Finished election with %v/%v", votes, len(nodes))
}

// requestVoteFromNode sends the given vote request to the given node. Writes a bool indicating whether votes were
// received to the success channel, and writes to the fallback channel as necessary
func (r *RaftNode) requestVoteFromNode(node *RemoteNode, req *RequestVoteRequest, success chan bool, fallback chan struct{}, group *sync.WaitGroup) {

	defer group.Done()

	ourOrigTerm := r.GetCurrentTerm()
	vote := false
	voteReply, err := node.RequestVoteRPC(r, req)
	r.Debug("response %v received from %v", voteReply, node.Id)
	if err != nil {
		r.Debug("Unreachable node in election; still trying for majority: %s (%s)", err, node)
	} else if r.updateTermIfNecessary(voteReply.GetTerm()) {
		// fallback to being a follower if someone had a higher term than us
		r.Debug("Someone had higher term; end candidacy (%d vs. our %d)", voteReply.GetTerm(), ourOrigTerm)
		fallback <- struct{}{}

	} else if voteReply.GetVoteGranted() {
		r.Debug("Candidate got one vote from %v", node.Id)
		vote = true
	} else {
		r.Debug("Candidate rejected by %v", node.Id)
	}
	// should always be non-blocking call since success is buffered
	success <- vote

}
