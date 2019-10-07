package raft

// __BEGIN_TA__
import (
	"reflect"
	"sort"
	"time"
)

// __END_TA__

// doLeader implements the logic for a Raft node in the leader state.
func (r *RaftNode) doLeader() stateFunction {
	r.Out("Transitioning to LEADER_STATE")
	r.State = LEADER_STATE
	// __BEGIN_TA__
	// Set current leader to ourselves
	r.Leader = r.GetRemoteSelf()

	// Init nextIndex and matchIndex maps
	r.leaderMutex.Lock()
	for _, node := range r.GetNodeList() {
		r.nextIndex[node.Id] = r.getLastLogIndex() + 1
		r.matchIndex[node.Id] = uint64(0)
	}
	r.leaderMutex.Unlock()

	// Now that we are the leader, append a no-op to the log.
	// From section 8 of the Raft paper.
	r.leaderMutex.Lock()
	leaderEntry := LogEntry{
		Index:  r.getLastLogIndex() + 1,
		TermId: r.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{},
	}
	r.appendLogEntry(leaderEntry)
	r.leaderMutex.Unlock()

	// Send out heartbeats
	fallback, _ := r.sendHeartbeats()

	if fallback {
		return r.doFollower
	}

	// Begin leader logic...
	heartbeat := time.Tick(r.config.HeartbeatTimeout)
	for {
		select {
		case msg := <-r.appendEntries:
			r.Out("Received append entries request in LEADER state")
			if _, fallback := r.handleAppendEntries(msg); fallback {
				return r.doFollower
			}

		case msg := <-r.requestVote:
			if r.handleCompetingRequestVote(msg) {
				return r.doFollower
			}

		case msg := <-r.registerClient:
			request := msg.request
			reply := msg.reply
			r.Out("Received client registration request %v %v\n", request, reply)

			r.leaderMutex.Lock()
			registerEntry := LogEntry{
				Index:  r.getLastLogIndex() + 1,
				TermId: r.GetCurrentTerm(),
				Type:   CommandType_CLIENT_REGISTRATION,
				Data:   []byte(r.GetRemoteSelf().Addr),
			}
			r.appendLogEntry(registerEntry)
			r.leaderMutex.Unlock()

			fallback, sentToMajority := r.sendHeartbeats()

			status := ClientStatus_OK
			if !sentToMajority {
				status = ClientStatus_REQ_FAILED
			}

			reply <- RegisterClientReply{
				Status:     status,
				ClientId:   registerEntry.Index,
				LeaderHint: r.GetRemoteSelf(),
			}

			if fallback {
				return r.doFollower
			}

		case msg := <-r.clientRequest:
			request := msg.request
			reply := msg.reply
			r.Out("Received client request %v %v\n", request, reply)

			cacheId := createCacheId(request.ClientId, request.SequenceNum)

			// We check the cache again, since it's possible that this is a
			// duplicate request that we've handled since the message was
			// placed on the channel.
			cachedReply, exists := r.GetCachedReply(*request)

			if exists {
				reply <- *cachedReply
				continue
			}

			r.leaderMutex.Lock()
			r.requestsMutex.Lock()

			prevReplyChan, exists := r.requestsByCacheId[cacheId]

			// Construct log entry
			requestIndex := r.getLastLogIndex() + 1
			requestEntry := LogEntry{
				Index:   requestIndex,
				TermId:  r.GetCurrentTerm(),
				Type:    CommandType_STATE_MACHINE_COMMAND,
				Command: request.StateMachineCmd,
				Data:    request.Data,
				CacheId: cacheId,
			}

			if exists {
				// If a request with this same cache ID has already happened, don't append
				// a new log entry. Instead, just wrap the channel for the old request,
				// and we'll respond to both the new and old one together once the request
				// has been processed.
				wrapperChan := make(chan ClientReply)

				go func() {
					clientReply := <-wrapperChan
					reply <- clientReply
					prevReplyChan <- clientReply
				}()

				// Add wrapper channel to the cache, so that when the reply is eventually
				// sent down this wrapper, it sends it down both the new and old channels
				// as well (via the go routine above.)
				r.requestsByCacheId[cacheId] = wrapperChan
			} else {
				// Otherwise, add the request to the leader's log
				r.requestsByCacheId[cacheId] = reply
				r.appendLogEntry(requestEntry)
			}

			r.requestsMutex.Unlock()
			r.leaderMutex.Unlock()

			// Send out heartbeats to attempt to commit either new log entry,
			// or the previously created log entry.
			fallback, _ := r.sendHeartbeats()

			// // Comment out below go routine to support timeouts for client requests:
			// go func() {
			// 	<-time.After(time.Second * 2)
			// 	r.requestsMutex.Lock()
			//
			// 	replyChan, exists := r.requestsByCacheId[cacheId]
			// 	if exists {
			// 		replyChan <- ClientReply{
			// 			Status:     ClientStatus_REQ_FAILED,
			// 			Response:   "Unable to commit log entry right now; will try again later",
			// 			LeaderHint: r.GetRemoteSelf(),
			// 		}
			// 		delete(r.requestsByCacheId, cacheId)
			// 	}
			//
			// 	r.requestsMutex.Unlock()
			// }()

			if fallback {
				return r.doFollower
			}

		case <-heartbeat:
			fallback, _ := r.sendHeartbeats()
			if fallback {
				return r.doFollower
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
	// leader state should do when it receives an incoming message on every
	// possible channel.
	return nil
	__END_STUDENT__ */
}

// __BEGIN_TA__

// HBResult represents the result of a single heartbeat sent to another node
type HBResult int

const (
	HB_FALLBACK HBResult = iota
	HB_SUCCESS
	HB_FAILURE
)

// sendHeartbeat sends a single heartbeat (as an AppendEntriesMsg) to another
// Raft node, and returns the result on a channel. If the heartbeat was
// successful, it updates the corresponding nextIndex and matchIndex locally.
func (r *RaftNode) sendHeartbeat(otherNode RemoteNode, currTerm uint64, result chan HBResult) {
	// Prepare list of log entries to send to other node...
	entries := make([]*LogEntry, 0)
	prevLogIndex := r.getLastLogIndex()
	r.leaderMutex.Lock()
	if r.getLastLogIndex() >= r.nextIndex[otherNode.Id] {
		for i := r.nextIndex[otherNode.Id]; i <= r.getLastLogIndex(); i++ {
			entries = append(entries, r.getLogEntry(i))
			if prevLogIndex > i {
				prevLogIndex = i
			}
		}
	}
	r.leaderMutex.Unlock()
	if prevLogIndex > 0 {
		prevLogIndex--
	}

	// Deduce the last known log entry and term of other node
	prevLogEntry := r.getLogEntry(prevLogIndex)
	prevLogTerm := prevLogEntry.TermId

	if len(entries) > 0 {
		r.Out("Sending %d entries to %v\n", len(entries), otherNode.Id)
	}

	// Prepare AppendEntriesRequest to send as heartbeat
	request := AppendEntriesRequest{
		Term:         r.GetCurrentTerm(),
		Leader:       r.GetRemoteSelf(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}

	// Send heartbeat
	reply, err := otherNode.AppendEntriesRPC(r, &request)

	if err != nil {
		r.Error("Failed to send heartbeat to %v (%v)\n", otherNode, err)
		result <- HB_FAILURE
		return
	}

	if reply.Term > currTerm {
		// We're in a previous term! Fall back to follower state
		r.Out("Heartbeat response indicates our term (%v) is not the most recent (%v)\n", currTerm, reply.Term)
		r.setVotedFor("")
		r.setCurrentTerm(reply.Term)
		result <- HB_FALLBACK
		return
	}

	r.Debug("reply.Success = %v, len(entries) = %v", reply.Success, len(entries))

	r.leaderMutex.Lock()
	defer r.leaderMutex.Unlock()

	if !reply.Success {
		// If the heartbeat failed, revert nextIndex and return HB_FAILURE
		r.Error("Failed to update %v during heartbeat\n", otherNode)

		r.Debug("Decrementing nextIndex from %v for node(%v)", r.nextIndex[otherNode.Id], otherNode.Id)
		if r.nextIndex[otherNode.Id] == 0 {
			panic("We're about to make nextIndex negative, this should never happen!")
		}
		r.nextIndex[otherNode.Id] -= uint64(1)

		result <- HB_FAILURE
	} else if len(entries) > 0 {
		numEntries := uint64(len(entries))
		// If the heartbeat succeeded, we just sent len(entries) to the other node;
		// let's update the corresponding nextIndex.
		nextIndex := r.nextIndex[otherNode.Id] + numEntries

		r.Debug("Updating nextIndex for node(%v) %v -> %v", otherNode.Id, r.nextIndex[otherNode.Id], nextIndex)
		r.nextIndex[otherNode.Id] = nextIndex

		// And let's update the matchIndex; the highest log entry replicated to the
		// other node is one less than the nextIndex.
		matchIndex := nextIndex - 1

		r.Debug("Updating matchIndex for node(%v) %v -> %v", otherNode.Id, r.matchIndex[otherNode.Id], matchIndex)
		r.matchIndex[otherNode.Id] = matchIndex

		result <- HB_SUCCESS
	} else {
		result <- HB_FAILURE
	}
}

// __END_TA__
// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (r *RaftNode) sendHeartbeats() (fallback, sentToMajority bool) {
	// __BEGIN_TA__
	currTerm := r.GetCurrentTerm()

	// Keep track of how many nodes we have successfully replicated entries to,
	// we can start at 1 since the leader already has the entries in question.
	successCount := 1
	quorum := (r.config.ClusterSize / 2) + 1

	// Array to store results of all heartbeats as switch-cases
	cases := make([]reflect.SelectCase, 0)

	// Iterate through nodes and send heartbeats...
	for _, node := range r.GetNodeList() {
		// The leader should not send a heartbeat to itself
		if node.Id == r.GetRemoteSelf().Id {
			continue
		}

		// Create channel to receive result from heartbeat (buffered to ensure we
		// always give up the leader mutex)
		result := make(chan HBResult, 1)
		resultCase := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(result)}
		cases = append(cases, resultCase)

		// Send individual heartbeat to other node
		go r.sendHeartbeat(node, currTerm, result)
	}

	for len(cases) > 0 {
		chosen, value, _ := reflect.Select(cases)
		switch HBResult(value.Int()) {
		case HB_FALLBACK:
			// We're in a previous term! Fall back to follower state
			return true, false
		case HB_SUCCESS:
			successCount++
		case HB_FAILURE:
			// Failed this heartbeat. Nothing to do.
		}

		cases = append(cases[:chosen], cases[chosen+1:]...)
	}

	r.leaderMutex.Lock()

	// If we achieved a majority, print out current state
	debug := successCount >= quorum
	if debug {
		r.Out("commitIndex: %v\n", r.commitIndex)
		r.Out("matchIndex:")
		for nodeId, val := range r.matchIndex {
			r.Out("\t[%v] %v\n", nodeId, val)
		}

		r.Out("LogCache:\n")
		for i, entry := range r.logCache {
			r.Out("\tidx:%v, entry.Index:%v, entry.TermId:%v\n", i, entry.Index, entry.TermId)
		}
	}

	// Once we have replicated an entry in our current term to the majority of
	// nodes, we proceed to commit all log entries up to it.

	// If there exists an N such that
	//   1) N > commitIndex,
	//   2) log[N].term == currentTerm:
	//   3) a majority of matchIndex[i] >= N,
	// then set commitIndex = N
	r.matchIndex[r.GetRemoteSelf().Id] = r.getLastLogIndex()
	matchIndexValues := make([]uint64, 0)
	for _, miVal := range r.matchIndex {
		matchIndexValues = append(matchIndexValues, miVal)
	}
	sort.Sort(UInt64Slice(matchIndexValues))
	N := matchIndexValues[r.config.ClusterSize-quorum]

	r.leaderMutex.Unlock()

	if debug {
		r.Out("Match indexes %v give us N(%v)\n", matchIndexValues, N)
	}

	// Satisfy condition 1
	if N > r.commitIndex && N <= r.getLastLogIndex() {
		if debug {
			r.Out("N(%v) > commitIdx(%v)\n", N, r.commitIndex)
		}
		// Partially satisfies condition 3
		entry := r.getLogEntry(N)
		if debug {
			r.Out("entry.TermId(%v) == currTerm(%v)\n", entry.TermId, r.GetCurrentTerm())
		}
		// Satisfy condition 2
		if (entry != nil) && (entry.TermId == r.GetCurrentTerm()) {
			if debug {
				r.Out("Found quorum for N(%v)", N)
			}
			r.Out("Updating commitIndex from %v -> %v\n", r.commitIndex, N)

			for N > r.lastApplied {
				r.lastApplied++
				r.processLogEntry(*r.getLogEntry(r.lastApplied))
			}

			r.commitIndex = N
		}
	}

	// All is right with the world, we stay in leader state.
	sentToMajority = successCount >= quorum
	fallback = false
	return
	// __END_TA__
	/* __BEGIN_STUDENT__
	// TODO: Students should implement this method
	return true, true
	__END_STUDENT__ */
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (r *RaftNode) processLogEntry(entry LogEntry) ClientReply {
	Out.Printf("Processing log entry: %v\n", entry)

	status := ClientStatus_OK
	response := ""
	var err error

	// Apply command on state machine
	if entry.Type == CommandType_STATE_MACHINE_COMMAND {
		response, err = r.stateMachine.ApplyCommand(entry.Command, entry.Data)
		if err != nil {
			status = ClientStatus_REQ_FAILED
			response = err.Error()
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		Response:   response,
		LeaderHint: r.GetRemoteSelf(),
	}

	// Add reply to cache
	if entry.CacheId != "" {
		r.CacheClientReply(entry.CacheId, reply)
	}

	// Send reply to client
	r.requestsMutex.Lock()
	replyChan, exists := r.requestsByCacheId[entry.CacheId]
	if exists {
		replyChan <- reply
		delete(r.requestsByCacheId, entry.CacheId)
	}
	r.requestsMutex.Unlock()

	return reply
}
