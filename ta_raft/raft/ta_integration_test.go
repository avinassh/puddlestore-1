package raft

import (
	"crypto/md5"
	"fmt"
	"os/user"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/raft/hashmachine"
)

func TestStandardFlow(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{4001, 4002, 4003, 4004, 4005})

	// Shutdown all nodes once finished
	defer cleanupStudentCluster(nodes)

	if err != nil {
		t.Fatalf("Could not create cluster: %v", err)
	}

	leader, leaderTerm := startTestCluster(nodes, t)

	time.Sleep(time.Second * WAIT_PERIOD)
	newLeaderTerm := leader.GetCurrentTerm()
	if newLeaderTerm != leaderTerm {
		t.Errorf("After sleeping for five seconds, there are new terms, despite no cluster activity. The old term was %v, the new term is %v", leaderTerm, newLeaderTerm)
		return
	}
}

func TestExtendedFlow(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{4006, 4007, 4008, 4009, 4010})

	// Shutdown all nodes once finished
	defer cleanupStudentCluster(nodes)

	if err != nil {
		t.Fatalf("Could not create cluster: %v", err)
	}

	leader, _ := startTestCluster(nodes, t)

	req := RegisterClientRequest{}
	result, _ := leader.RegisterClient(&req)

	if result.Status != ClientStatus_OK {
		t.Errorf("Registering with the leader should return OK when they receive a RegisterClient; instead returned %v", result.Status)
		return
	}

	initialHash := []byte("hello")
	clientReq := ClientRequest{
		result.ClientId,
		1,
		hashmachine.HASH_CHAIN_INIT,
		initialHash,
	}

	clientResult, _ := leader.ClientRequest(&clientReq)

	if clientResult.Status != ClientStatus_OK {
		t.Errorf("Sending a ClientRequest to the leader should return OK; instead returned %v", clientResult.Status)
		return
	}

	var i uint64
	for i = 2; i < 100; i++ {
		parReq := ClientRequest{
			result.ClientId,
			i,
			hashmachine.HASH_CHAIN_ADD,
			[]byte{},
		}

		requestResult, _ := leader.ClientRequest(&parReq)
		if requestResult.Status != ClientStatus_OK {
			t.Errorf("Sending a ClientRequest to the leader should return OK; instead returned %v", clientResult.Status)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	if !logsCorrect(leader, nodes) {
		t.Fatalf("The followers don't have the same log as the leader.")
	}
}

func TestCatchupShutdownFollower(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{4011, 4012, 4013, 4014, 4015})

	if err != nil {
		t.Fatalf("Could not create cluster: %v", err)
	}
	defer cleanupStudentCluster(nodes)

	leader, _ := startTestCluster(nodes, t)
	leaderTerm := leader.GetCurrentTerm()
	leaderId := leader.Id

	follower := findFollower(nodes)
	followerPort := follower.port
	// shutdown follower
	t.Logf("Shutting down %v, port %v", follower.Id, followerPort)
	follower.GracefulExit()
	follower.server.GracefulStop()
	time.Sleep(WAIT_PERIOD * time.Second) // wait for shutdown
	for i := 0; i < 9; i++ {
		leader.leaderMutex.Lock()
		logEntry := LogEntry{
			Index:  leader.getLastLogIndex() + 1,
			TermId: leader.GetCurrentTerm(),
			Type:   CommandType_NOOP,
			Data:   []byte{5, 6, 7, 8},
		}
		leader.appendLogEntry(logEntry)
		leader.leaderMutex.Unlock()
	}
	time.Sleep(WAIT_PERIOD * time.Second) // wait for entries to be propagated
	t.Logf("Starting up node\n")
	// bring follower back up
	config := DefaultConfig()
	config.ClusterSize = 5
	// Use a path in /tmp/ so we use local disk and not NFS
	curUser, _ := user.Current()
	config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	if runtime.GOOS == "windows" {
		config.LogPath = "raftlogs/"
	}
	resurrectedNode, err := CreateNode(followerPort, leader.GetRemoteSelf(), new(hashmachine.HashMachine), config)
	if err != nil {
		t.Errorf("Failed to bring back up follower with port %v. err: %v", followerPort, err)
	}

	time.Sleep(WAIT_PERIOD * time.Second) // wait for cluster to stabilize

	newLeader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	if leaderId != newLeader.Id {
		t.Errorf("Leader should not have changed! was %v, now %v", leaderId, newLeader.Id)
	}

	newLeaderTerm := newLeader.GetCurrentTerm()
	if newLeaderTerm > leaderTerm {
		t.Errorf("Leader's term shouldn't have increased: was %v, now %v", leaderTerm, newLeaderTerm)
	}

	for _, node := range nodes {
		if node.Id == resurrectedNode.Id || node.Id == newLeader.Id {
			continue
		} else {
			node.GracefulExit()
		}
	}
	newLeader.GracefulExit()
}

func TestStatePersistenceAfterShutdown(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{4011, 4012, 4013, 4014, 4015})

	if err != nil {
		t.Fatalf("Could not create cluster: %v", err)
	}
	defer cleanupStudentCluster(nodes)

	leader, _ := startTestCluster(nodes, t)

	follower := findFollower(nodes)
	followerPort := follower.port
	oldFollowerStableState := follower.stableState
	oldFollowerLogCache := follower.logCache
	// shutdown follower
	t.Logf("Shutting down %v, port %v", follower.Id, followerPort)
	follower.GracefulExit()
	follower.server.GracefulStop()
	time.Sleep(WAIT_PERIOD * time.Second) // wait for shutdown
	fmt.Printf("NODE SHUT DOWN\n")
	t.Logf("Starting up node\n")
	// bring follower back up
	config := DefaultConfig()
	config.ClusterSize = 5
	// Use a path in /tmp/ so we use local disk and not NFS
	curUser, _ := user.Current()
	config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	if runtime.GOOS == "windows" {
		config.LogPath = "raftlogs/"
	}
	resurrectedNode, err := CreateNode(followerPort, leader.GetRemoteSelf(), new(hashmachine.HashMachine), config)
	if err != nil {
		t.Errorf("Failed to bring back up follower with port %v. err: %v", followerPort, err)
	}

	// Pause world so that nothing in node's state will change while we check it
	resurrectedNode.NetworkPolicy.PauseWorld(true)
	newFollowerStableState := resurrectedNode.stableState
	newFollowerLogCache := resurrectedNode.logCache
	time.Sleep(WAIT_PERIOD * time.Second) // wait for cluster to stabilize

	if !checkStableStateEquality(oldFollowerStableState, newFollowerStableState) {
		t.Errorf("Stable state does not match before and after node shutdown! was: %v, now %v\n", oldFollowerStableState, newFollowerStableState)
	}

	if !checkLogCacheEquality(oldFollowerLogCache, newFollowerLogCache) {
		t.Errorf("Log cache does not match before and after node shutdown! was: %v, now %v\n", oldFollowerLogCache, newFollowerLogCache)
	}
	resurrectedNode.server.Stop()
	go func(node *RaftNode) {
		node.GracefulExit()
		node.RemoveLogs()
	}(resurrectedNode)
}

//*
func TestClientInteractions(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{7001, 7002, 7003, 7004, 7005})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupStudentCluster(nodes)

	time.Sleep(time.Second * WAIT_PERIOD) // Long enough for timeout+election

	// Find leader to ensure that election has finished before we grab a follower
	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	leaderRemoteNode := leader.GetRemoteSelf()

	follower := findFollower(nodes)

	// Test: Request to follower should return NOT_LEADER
	req := RegisterClientRequest{}
	result, _ := follower.RegisterClient(&req)

	if result.Status != ClientStatus_NOT_LEADER {
		t.Errorf("Followers should return NOT_LEADER when they receive a RegisterClient; instead returned %v", result.Status)
		return
	}

	if leader == nil || *result.LeaderHint != *leaderRemoteNode {
		t.Errorf("Follower doesn't return correct leader address to RegisterClient. Should be %v, but is %v", leaderRemoteNode, result.LeaderHint)
		return
	}

	// Test: Returning OK should mean that client registration entry is now on majority of followers

	result, _ = leader.RegisterClient(&req)

	if result.Status != ClientStatus_OK {
		t.Errorf("Registering with the leader should return OK when they receive a RegisterClient; instead returned %v", result.Status)
		return
	}

	propagatedTo := 0
	for _, node := range nodes {
		nodeIndex := node.getLastLogIndex()
		if nodeIndex >= result.ClientId {
			nodeEntry := node.getLogEntry(result.ClientId)
			if nodeEntry == nil {
				t.Errorf("The log entry for the client's registration should not be nil")
				return
			}
			if nodeEntry.Type != CommandType_CLIENT_REGISTRATION {
				t.Errorf("The log entry for the client registration should have command type CLIENT_REGISTRATION; instead has %v", nodeEntry.Command)
				return
			}
			propagatedTo++
		}
	}

	quorum := (leader.config.ClusterSize / 2) + 1

	if propagatedTo < quorum {
		t.Errorf("Leader responded with OK to client before propagating the log entry to a quorum of nodes (was only sent to %v)", propagatedTo)
		// Don't return here, since this doesn't affect future tests aside from the other propagation
	}

	// Test: Repeating same request with same session ID should return same result
	initialHash := []byte("hello")
	clientReq := ClientRequest{
		result.ClientId,
		1,
		hashmachine.HASH_CHAIN_INIT,
		initialHash,
	}

	clientResult, _ := follower.ClientRequest(&clientReq)

	if clientResult.Status != ClientStatus_NOT_LEADER {
		t.Errorf("Followers should return NOT_LEADER when they receive a ClientRequest; instead returned %v", clientResult.Status)
		return
	}

	if *clientResult.LeaderHint != *leaderRemoteNode {
		t.Errorf("Follower doesn't return correct leader address to ClientRequest. Should be %v, but is %v", leaderRemoteNode, result.LeaderHint)
		return
	}

	clientResult, _ = leader.ClientRequest(&clientReq)

	if clientResult.Status != ClientStatus_OK {
		t.Errorf("Sending a ClientRequest to the leader should return OK; instead returned %v", clientResult.Status)
		return
	}

	initIndex := leader.getLastLogIndex()

	propagatedTo = 0
	for _, node := range nodes {
		nodeIndex := node.getLastLogIndex()
		if nodeIndex >= initIndex {
			nodeEntry := node.getLogEntry(initIndex)
			if nodeEntry == nil {
				t.Errorf("The log entry for the client's request should not be nil")
				return
			}
			if nodeEntry.Command != hashmachine.HASH_CHAIN_INIT {
				t.Errorf("The log entry for the client's request should have command type HASH_CHAIN_INIT; instead has %v", nodeEntry.Command)
				return
			}
			propagatedTo++
		}
	}

	if propagatedTo < quorum {
		t.Errorf("Leader responded with OK to client before propagating the log entry to a quorum of nodes (was only sent to %v)", propagatedTo)
		// Don't return here, since this doesn't affect future tests
	}

	newClientResult, _ := leader.ClientRequest(&clientReq)

	if newClientResult.Status != ClientStatus_OK {
		t.Errorf("Repeated ClientRequest to the leader should return OK; instead returned %v with response string \"%v\"", newClientResult.Status, newClientResult.Response)
		return
	}

	// Test: Performing the same requests concurrently should all return the same result
	// Note: This is hard to test perfectly, but passing it suggests that the person's
	// most likely considered this case.
	parReq := ClientRequest{
		result.ClientId,
		2,
		hashmachine.HASH_CHAIN_ADD,
		[]byte{},
	}
	cases := make([]reflect.SelectCase, CONCURRENT_REQUESTS)
	for i := 0; i < CONCURRENT_REQUESTS; i++ {
		result := make(chan ClientReply)
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(result)}

		go func() {
			parResult, _ := leader.ClientRequest(&parReq)
			result <- parResult
		}()
	}

	expectedHash := md5.Sum(initialHash)
	expectedHashString := fmt.Sprintf("%v", expectedHash[:])

	for len(cases) > 0 {
		chosen, value, _ := reflect.Select(cases)
		parReply := value.Interface().(ClientReply)

		if parReply.Status != ClientStatus_OK {
			t.Errorf("Concurrent ClientRequest to the leader should return OK; instead returned %v with response string \"%v\"", parReply.Status, parReply.Response)
			// We don't return here, since this doesn't affect the next tests
		}

		if expectedHashString != parReply.Response {
			t.Errorf("Repeated ClientRequest to the leader should return %v; instead returned with response string \"%v\"", expectedHashString, parReply.Response)
			// We don't return here, since this doesn't affect the next tests.
		}

		cases = append(cases[:chosen], cases[chosen+1:]...)
	}

	// Test: If leader is partitioned off and rejoins, requests
	// sent to that leader during partition should return NOT_LEADER
	// (Alternatively, could time out. If this happens, you'll need
	// to check the source to make sure they're doing this when becoming
	// a follower.)
	leader.NetworkPolicy.PauseWorld(true)

	failedReq := ClientRequest{
		result.ClientId,
		3,
		hashmachine.HASH_CHAIN_ADD,
		[]byte{},
	}

	failedReply := make(chan ClientReply)
	go func() {
		failedResult, _ := leader.ClientRequest(&failedReq)
		failedReply <- failedResult
	}()

	time.Sleep(time.Second * WAIT_PERIOD) // wait for new election to happen
	leader.NetworkPolicy.PauseWorld(false)

	shouldBeFailed := <-failedReply

	if shouldBeFailed.Status != ClientStatus_NOT_LEADER {
		t.Errorf("ClientRequest to partitioned leader should return NOT_LEADER on rejoining; instead returned %v with response string \"%v\"", shouldBeFailed.Status, shouldBeFailed.Response)
		return
	}
}
