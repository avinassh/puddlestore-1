package raft

import (
	"sync"
	"time"
)

// INSTRUCTIONS
// perform any initial work, and then consider what a node in the
// leader state should do when it receives an incoming message on every
// possible channel.

// CHANGE OF DESIGN -- HAVE LEADER LOOP OVER NODES AND INITIATE A TIMER HEARTBEAT
// WITH EACH OF THEM INDIVIDUALLY

func (r *RaftNode) doLeader() stateFunction {

	r.Out("Transitioning to LEADER_STATE; TIMESTAMP: %v", time.Now())

	r.initLeaderState()

	wg := sync.WaitGroup{}
	nodes := r.GetNodeList()
	fallback := make(chan struct{})
	done := make(chan struct{})
	// begins heartbeats
	for i := range nodes {
		if nodes[i].GetAddr() != r.GetRemoteSelf().GetAddr() {
			wg.Add(1)
			go r.sendHeartBeatsToNode(&nodes[i], fallback, done, &wg)
		}
	}
	// begin lastApplied
	wg.Add(1)
	go r.checkLastApplied(done, &wg)

	// close all sync mechanisms
	defer wg.Wait()
	defer close(done)
	defer r.Debug("Exiting from LEADER_STATE")

	for {
		//r.Verbose("Waiting on event...")
		select {
		case exit := <-r.gracefulExit:
			if exit {
				r.Out("Shutting down")
				return nil
			}
		case msg := <-r.appendEntries:
			if _, fb := r.handleAppendEntries(msg); fb {
				return r.doFollower
			}
		case msg := <-r.requestVote:
			r.Debug("Request vote received from %v", msg.request.Candidate)
			// if term is greater, update it and revert to follower
			if r.updateTermIfNecessary(msg.request.GetTerm()) {
				// now we're essentially a follower, so see if we should vote for the new leader
				r.handleRequestVote(msg)
				return r.doFollower

			} else {
				// if we're the rightful leader, just ignore the request and don't vote
				res := RequestVoteReply{r.GetCurrentTerm(), false}
				msg.reply <- res
			}
		case msg := <-r.registerClient:
			r.Out("RegisterClient received")
			r.handleRegisterClientAsLeader(msg)
		case msg := <-r.clientRequest:
			r.handleClientRequestAsLeader(msg)
		case <-fallback:
			// some node has higher term than us so convert to follower
			return r.doFollower
		}
	}

	return nil
}

// initLeaderState initializes all members of a Node to their leader counterparts
// LOCKS LEADERMUTEX
func (r *RaftNode) initLeaderState() {
	r.leaderMutex.Lock()
	r.nodeMutex.Lock()
	defer r.leaderMutex.Unlock()
	defer r.nodeMutex.Unlock()

	r.State = LEADER_STATE
	r.appendNoOpLogEntry()
	r.Leader = r.GetRemoteSelf()
	for _, node := range r.GetNodeList() {
		r.matchIndex[node.Addr] = 0
		r.nextIndex[node.Addr] = r.getLastLogIndex() + 1
	}
}

// Appends a NO-OP log entry to the log; called by leaders when first elected
func (r *RaftNode) appendNoOpLogEntry() {
	entry := LogEntry{
		Index:  r.getLastLogIndex() + 1,
		TermId: r.GetCurrentTerm(),
		Type:   CommandType_NOOP,
	}
	if err := r.appendLogEntry(entry); err != nil {
		r.Error("Couldn't append no-op entry on election: %v", err)
	}
}

// STUDENT WRITTEN
func (r *RaftNode) handleClientRequestAsLeader(msg ClientRequestMsg) {
	r.leaderMutex.Lock()
	r.requestsMutex.Lock()
	defer r.leaderMutex.Unlock()
	defer r.requestsMutex.Unlock()

	// check for cached reply or create a new request
	if cachedReply, existed := r.GetCachedReply(*msg.request); existed {
		msg.reply <- *cachedReply

	} else {
		// creates new log entry
		entry := LogEntry{
			Index:   r.getLastLogIndex() + 1,
			TermId:  r.GetCurrentTerm(),
			Type:    CommandType_STATE_MACHINE_COMMAND,
			Command: msg.request.StateMachineCmd,
			Data:    msg.request.Data,
			CacheId: createCacheId(msg.request.ClientId, msg.request.SequenceNum),
		}
		// must append it to log
		// new LogEntry eventually will be broadcast via heartbeats
		// will feed correct response into channel in processLogValue
		// to return values to client
		r.requestsByCacheId[entry.GetCacheId()] = msg.reply
		r.appendLogEntry(entry)
	}
}

// STUDENT WRITTEN
func (r *RaftNode) handleRegisterClientAsLeader(msg RegisterClientMsg) {

	r.leaderMutex.Lock()
	r.requestsMutex.Lock()
	defer r.leaderMutex.Unlock()
	defer r.requestsMutex.Unlock()

	// creates appropriate entry and appends it
	// ClientID is index of new entry in log
	entry := LogEntry{
		Index:  r.getLastLogIndex() + 1,
		TermId: r.GetCurrentTerm(),
		Type:   CommandType_CLIENT_REGISTRATION,
	}

	// delegates work to processLogEntry
	r.registrationsByClientID[entry.GetIndex()] = msg.reply
	r.appendLogEntry(entry)
	r.Out("Appended RegisterClientReq to log")
}

/* INSTRUCTIONS
sendHeartbeats is used by the leader to send out heartbeats to each of
the other nodes. It returns true if the leader should fall back to the
follower state. (This happens if we discover that we are in an old term.)

If another node isn't up-to-date, then the leader should attempt to
update them, and, if an index has made it to a quorum of nodes, commit
up to that index. Once committed to that index, the replicated state
machine should be given the new log entries via processLogEntry.
*/

// HEARTBEAT DESIGN
// have high level sendHeartbeats delegates to multiple synchronous goroutines that
// send the appropriate request at *their current time* to the nodes. There is
// no guarantee that they send consistent information with respect to the leader's logs.
// These independent goroutines will update the value of matchIndex for their appropriate nodes
// We will increment commitIndex by running a concurrent goroutine that loops just as often as the
// heartbeat that checks the local matchIndex map for valid increments.

/*
Send one set of heartbeats each timer interval, runs until ticker is closed.
done: channel that is closed when heartbeats are done
fallback: channel to send fallback information to
wg: waitgroup to sync
*/

/*
STUDENT WRITTEN
Sends a sequence of heartbeats to a node
Successful - will write the ID of the node it successfully sent a heartbeat to
Unsuccessful - write if heartbeat is unsuccessful for whatever reason
Terminal - write ID if we are a stale leader (follower has a higher term)
*/
func (r *RaftNode) sendHeartBeatsToNode(node *RemoteNode, fallback, done chan struct{}, wg *sync.WaitGroup) {

	// send initial heartbeat on leader election
	r.Debug("Sending initial leader heartbeat to %v", node.Id)
	r.sendHeartBeat(node, fallback)

	heartbeatTicker := time.NewTicker(r.config.HeartbeatTimeout)
	defer wg.Done()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			r.sendHeartBeat(node, fallback)
		case <-done:
			return
		}
	}
}

// Sends a single heartbeat to the given RemoteNode
func (r *RaftNode) sendHeartBeat(node *RemoteNode, fallback chan struct{}) {
	// generate request
	r.leaderMutex.Lock()
	logEntries := r.getLogEntriesFromCache()
	lastIndex := r.getLastLogIndex()
	prevIndex := r.nextIndex[node.GetAddr()] - 1
	req := AppendEntriesRequest{
		Term:         r.GetCurrentTerm(),
		Leader:       r.GetRemoteSelf(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  logEntries[prevIndex].GetTermId(),
		Entries:      logEntries[prevIndex+1:], //slice ensures that only elms after prevIndex are sent
		LeaderCommit: r.commitIndex,
	}
	r.leaderMutex.Unlock()

	res, err := node.AppendEntriesRPC(r, &req)
	if err != nil {
		//r.Debug("Heartbeat from %v errored", node)
		return
	}

	r.leaderMutex.Lock()
	if r.updateTermIfNecessary(res.GetTerm()) {
		fallback <- struct{}{}
	} else if !res.GetSuccess() {
		// if further decrementation is possible, send again in the next round of heartbeats
		if req.PrevLogIndex != 0 {
			// if for some reason the heartbeat doesn't work -- i.e should we be faster about retrying
			r.nextIndex[node.GetAddr()]--
		}
	} else {
		// successfully updated follower to be length of log in request (not necessarily
		// representative of current state of log)
		nextIndex := lastIndex + 1
		if r.nextIndex[node.GetAddr()] < nextIndex {
			r.nextIndex[node.GetAddr()] = nextIndex
		}
		if r.matchIndex[node.GetAddr()] < lastIndex {
			r.matchIndex[node.GetAddr()] = lastIndex
		}
	}
	r.leaderMutex.Unlock()
}

/*
STUDENT WRITTEN
With an interval equal to heartbeat, check for possible updates to last applied
*/
func (r *RaftNode) checkLastApplied(done chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	lastAppliedTicker := time.NewTicker(r.config.HeartbeatTimeout)
	defer lastAppliedTicker.Stop()
	for {
		select {
		case <-lastAppliedTicker.C:
			r.checkLastAppliedHelper()
		case <-done:
			return
		}
	}
}

// STUDENT WRITTEN
// Guts of checkLastApplied; encapsulated out for testing
func (r *RaftNode) checkLastAppliedHelper() (newCommitIndex uint64) {
	r.leaderMutex.Lock()
	defer r.leaderMutex.Unlock()

	nodes := r.GetNodeList()
	newCommitIndex = r.getLastLogIndex()
	for newCommitIndex > r.commitIndex {
		// latest log entry must be of current term to commit
		if r.getLogEntry(newCommitIndex).GetTermId() == r.GetCurrentTerm() {
			// find number of nodes that have matched up to newCommitIndex
			// we always match, so start at 1
			numMatched := 1
			for _, n := range nodes {
				if n.Addr != r.GetRemoteSelf().Addr && r.matchIndex[n.Addr] >= newCommitIndex {
					numMatched++
				}
			}
			// if this is a majority (in matchIndex) update commitIndex to newCommitIndex
			if numMatched > len(nodes)/2 {
				// we know that newCommitIndex is greater than commitIndex because we hold the leader mutex
				r.commitUntil(newCommitIndex)
				return
			}
		}
		newCommitIndex--
	}
	return
}

// STUDENT WRITTEN
// WRAP IN LEADER MUTEX CALLS
func (r *RaftNode) getLogEntriesFromCache() []*LogEntry {
	logEntries := make([]*LogEntry, len(r.logCache))
	for i := range r.logCache {
		logEntries[i] = &r.logCache[i]
	}
	return logEntries
}

// Calls processLogEntry on everything up to and including newIndex.
// Sets commitIndex to be newIndex.
// WRAP IN LEADER MUTEX CALLS
func (r *RaftNode) commitUntil(newIndex uint64) {
	r.Debug("commitIndex updated to %d", newIndex)
	until := min(newIndex, r.getLastLogIndex())
	for i := r.commitIndex + 1; i <= until; i++ {
		r.processLogEntry(*r.getLogEntry(i))
	}
	r.commitIndex = until
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response. Returns EMPTY ClientReply{}
// if the LogEntry was a RegisterClient
// STUDENT WRITTEN
func (r *RaftNode) processLogEntry(entry LogEntry) ClientReply {
	Out.Printf("Processing log entry: %v\n", entry)
	status := ClientStatus_OK
	response := ""
	var err error

	switch entry.GetType() {
	case CommandType_STATE_MACHINE_COMMAND:
		response, err = r.stateMachine.ApplyCommand(entry.Command, entry.Data)
		r.lastApplied = entry.Index
		if err != nil {
			status = ClientStatus_REQ_FAILED
			response = err.Error()
		}
		// Construct reply
		reply := ClientReply{
			Status:     status,
			Response:   response,
			LeaderHint: r.GetRemoteSelf(),
		}

		// Send reply to client
		r.requestsMutex.Lock()
		replyChan, exists := r.requestsByCacheId[entry.CacheId]
		if exists {
			r.Out("Sending reply %v to client", reply)
			replyChan <- reply
			delete(r.requestsByCacheId, entry.CacheId)
		} else {
			badReply := ClientReply{ClientStatus_REQ_FAILED,
				"Request already fulfilled or bad sequence number", nil}
			r.Out("Sending BAD reply %v to client", badReply)
			replyChan <- badReply
		}
		r.requestsMutex.Unlock()

		// Add reply to cache
		if entry.CacheId != "" {
			r.CacheClientReply(entry.CacheId, reply)
		}

		return reply

	case CommandType_CLIENT_REGISTRATION:
		// creates reply
		reply := RegisterClientReply{
			Status:   ClientStatus_OK,
			ClientId: entry.GetIndex(),
		}

		// Send reply to client
		r.requestsMutex.Lock()
		replyChan, exists := r.registrationsByClientID[entry.GetIndex()]
		if exists {
			replyChan <- reply
			delete(r.registrationsByClientID, entry.GetIndex())
		}
		r.requestsMutex.Unlock()
		return ClientReply{}

		// CommandType_INIT - only added at start
		// CommandType_NOOP - not explicitly handled by state machine
	}
	return ClientReply{}
}
