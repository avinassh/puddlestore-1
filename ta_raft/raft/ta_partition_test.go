package raft

import (
	"fmt"
	"testing"
	"time"
)

//*
func TestThreeWayPartition311(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{5001, 5002, 5003, 5004, 5005})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupStudentCluster(nodes)
	time.Sleep(time.Second * WAIT_PERIOD) // Long enough for timeout+election

	// Partition off leader
	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}
	leaderTerm := leader.GetCurrentTerm()
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(time.Second * 2) // Wait, just in case the partitioned leader attempts anything

	// Partition off follower
	follower := findFollower(nodes)
	followerTerm := follower.GetCurrentTerm()
	follower.NetworkPolicy.PauseWorld(true)

	// Test: Make sure that the original leader remains a leader post-partition
	if leader.State != LEADER_STATE {
		t.Errorf("Leader should remain leader even when partitioned off")
		return
	}

	pausedLeaderTerm := leader.GetCurrentTerm()

	if leaderTerm != pausedLeaderTerm {
		t.Errorf("Leader's term should remain the same after partition. Went from %v to %v", leaderTerm, pausedLeaderTerm)
		return
	}

	time.Sleep(time.Second * WAIT_PERIOD) // Finish waiting for rest of cluster to elect a new leader

	// Test: Make sure partitioned follower has transitioned to candidate state and has increased term
	if follower.State != CANDIDATE_STATE {
		t.Errorf("Partitioned follower has not transitioned to candidate after %v seconds", WAIT_PERIOD)
	}

	partitionedFollowerTerm := follower.GetCurrentTerm()
	if partitionedFollowerTerm <= followerTerm {
		t.Errorf("The term of the partitioned follower, which is now a candidate, should have increased")
		return
	}

	// Test: Make sure new leader is elected out of remaining 3 nodes
	partitionedLeaders := make([]*RaftNode, 0)

	for _, node := range nodes {
		if node == leader || node == follower {
			continue
		}

		if node.State == LEADER_STATE {
			partitionedLeaders = append(partitionedLeaders, node)
		}
	}

	if len(partitionedLeaders) == 0 {
		t.Errorf("The rest of the cluster didn't elect a new leader after %v seconds!", WAIT_PERIOD)
		return
	} else if len(partitionedLeaders) > 1 {
		t.Errorf("The rest of the cluster elected more than one leader after %v seconds! They elected %v leaders", WAIT_PERIOD, len(partitionedLeaders))
		return
	}

	newLeader := partitionedLeaders[0]
	newLeaderTerm := newLeader.GetCurrentTerm()

	if newLeaderTerm <= leaderTerm {
		t.Errorf("The term of the new leader should be higher than that of the old leader")
		return
	}

	// Test: add new log entry to rest of cluster, and wait until it's commited
	newLeader.leaderMutex.Lock()
	logEntry := LogEntry{
		Index:  newLeader.getLastLogIndex() + 1,
		TermId: newLeader.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	newLeader.appendLogEntry(logEntry)
	newLeader.leaderMutex.Unlock()

	time.Sleep(time.Second * 2) // Finish waiting for cluster to settle

	// Bring old leader back into cluster
	leader.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WAIT_PERIOD) // Finish waiting for cluster to settle

	// Test: Make sure that the leader becomes a follower, and the current leader remains the leader
	postMergeLeader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	leaderNewState := leader.State
	if leaderNewState != FOLLOWER_STATE {
		t.Errorf("Old leader should fall back to the follower state after rejoining (was in %v state)", leaderNewState)
		return
	}

	leaderNewTerm := leader.GetCurrentTerm()
	leaderNewIndex := leader.getLastLogIndex()

	pmLeaderTerm := postMergeLeader.GetCurrentTerm()
	pmLeaderIndex := postMergeLeader.getLastLogIndex()

	if leaderNewTerm != pmLeaderTerm {
		t.Errorf("Old leader (%v) should now be in the same term as new leader (%v)", leaderNewTerm, pmLeaderTerm)
		return
	}

	if leaderNewIndex != pmLeaderIndex {
		t.Errorf("Old leader (%v) should now have the same most recent index as new leader (%v)", leaderNewIndex, pmLeaderIndex)
		return
	}

	if postMergeLeader != newLeader {
		t.Errorf("Bringing the old leader back in should not disturb the new leader")
		return
	}

	// Bring back partitioned follower back into cluster
	follower.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WAIT_PERIOD) // Finish waiting for cluster to settle

	// Test: Make sure that the follower stays a follower, and the current leader remains the leader
	postMergeLeader, err = findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	followerNewState := follower.State
	if followerNewState != FOLLOWER_STATE {
		t.Errorf("Old follower should remain in follower state after rejoining (was in %v state)", followerNewState)
		return
	}

	followerNewTerm := follower.GetCurrentTerm()
	followerNewIndex := follower.getLastLogIndex()

	pmLeaderTerm = postMergeLeader.GetCurrentTerm()
	pmLeaderIndex = postMergeLeader.getLastLogIndex()

	if followerNewTerm != pmLeaderTerm {
		t.Errorf("Unpartitioned follower (%v) should now be in the same term as new leader (%v)", followerNewTerm, pmLeaderTerm)
		return
	}

	if followerNewIndex != pmLeaderIndex {
		t.Errorf("Unpartitioned follower (%v) should now have the same most recent index as new leader (%v)", followerNewIndex, pmLeaderIndex)
		return
	}

}

//*
func TestThreeWayPartition311_SimultaneousRejoin(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{5006, 5007, 5008, 5009, 5010})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupStudentCluster(nodes)
	time.Sleep(time.Second * WAIT_PERIOD) // Long enough for timeout+election

	// Partition off leader
	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}
	leaderTerm := leader.GetCurrentTerm()
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(time.Second * 2) // Wait, just in case the partitioned leader attempts anything

	// Partition off follower
	follower := findFollower(nodes)
	followerTerm := follower.GetCurrentTerm()
	follower.NetworkPolicy.PauseWorld(true)

	// Test: Make sure that the original leader remains a leader post-partition
	if leader.State != LEADER_STATE {
		t.Errorf("Leader should remain leader even when partitioned off")
		return
	}

	pausedLeaderTerm := leader.GetCurrentTerm()

	if leaderTerm != pausedLeaderTerm {
		t.Errorf("Leader's term should remain the same after partition. Went from %v to %v", leaderTerm, pausedLeaderTerm)
		return
	}

	time.Sleep(time.Second * WAIT_PERIOD) // Finish waiting for rest of cluster to elect a new leader

	// Test: Make sure partitioned follower has transitioned to candidate state and has increased term
	if follower.State != CANDIDATE_STATE {
		t.Errorf("Partitioned follower has not transitioned to candidate after %v seconds", WAIT_PERIOD)
	}

	partitionedFollowerTerm := follower.GetCurrentTerm()
	if partitionedFollowerTerm <= followerTerm {
		t.Errorf("The term of the partitioned follower, which is now a candidate, should have increased")
		return
	}

	// Test: Make sure new leader is elected out of remaining 3 nodes
	partitionedLeaders := make([]*RaftNode, 0)

	for _, node := range nodes {
		if node == leader || node == follower {
			continue
		}

		if node.State == LEADER_STATE {
			partitionedLeaders = append(partitionedLeaders, node)
		}
	}

	if len(partitionedLeaders) == 0 {
		t.Errorf("The rest of the cluster didn't elect a new leader after %v seconds!", WAIT_PERIOD)
		return
	} else if len(partitionedLeaders) > 1 {
		t.Errorf("The rest of the cluster elected more than one leader after %v seconds! They elected %v leaders", WAIT_PERIOD, len(partitionedLeaders))
		return
	}

	newLeader := partitionedLeaders[0]
	newLeaderTerm := newLeader.GetCurrentTerm()

	if newLeaderTerm <= leaderTerm {
		t.Errorf("The term of the new leader should be higher than that of the old leader")
		return
	}

	// Test: add new log entry to rest of cluster, and wait until it's commited
	newLeader.leaderMutex.Lock()
	logEntry := LogEntry{
		Index:  newLeader.getLastLogIndex() + 1,
		TermId: newLeader.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	newLeader.appendLogEntry(logEntry)
	newLeader.leaderMutex.Unlock()

	time.Sleep(time.Second * 2) // Finish waiting for cluster to settle

	// Bring old leader and parititoned follower back into cluster
	leader.NetworkPolicy.PauseWorld(false)
	follower.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WAIT_PERIOD) // Finish waiting for cluster to settle

	// Test: Make sure that the leader becomes a follower, and the current leader remains the leader
	// Also, make sure that the follower stays a follower, and the current leader remains the leader
	postMergeLeader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	leaderNewState := leader.State
	if leaderNewState != FOLLOWER_STATE {
		t.Errorf("Old leader should fall back to the follower state after rejoining (was in %v state)", leaderNewState)
		return
	}

	followerNewState := follower.State

	if followerNewState != FOLLOWER_STATE {
		t.Errorf("Old follower should remain in follower state after rejoining (was in %v state)", followerNewState)
		return
	}

	leaderNewTerm := leader.GetCurrentTerm()
	leaderNewIndex := leader.getLastLogIndex()

	pmLeaderTerm := postMergeLeader.GetCurrentTerm()
	pmLeaderIndex := postMergeLeader.getLastLogIndex()

	if leaderNewTerm != pmLeaderTerm {
		t.Errorf("Old leader (%v) should now be in the same term as new leader (%v)", leaderNewTerm, pmLeaderTerm)
		return
	}

	if leaderNewIndex != pmLeaderIndex {
		t.Errorf("Old leader (%v) should now have the same most recent index as new leader (%v)", leaderNewIndex, pmLeaderIndex)
		return
	}

	if postMergeLeader != newLeader {
		t.Errorf("Bringing the old leader back in should not disturb the new leader")
		return
	}

	followerNewTerm := follower.GetCurrentTerm()
	followerNewIndex := follower.getLastLogIndex()

	if followerNewTerm != pmLeaderTerm {
		t.Errorf("Unpartitioned follower (%v) should now be in the same term as new leader (%v)", followerNewTerm, pmLeaderTerm)
		return
	}

	if followerNewIndex != pmLeaderIndex {
		t.Errorf("Unpartitioned follower (%v) should now have the same most recent index as new leader (%v)", followerNewIndex, pmLeaderIndex)
		return
	}

}

//*
func TestThreeWayPartition221(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{5011, 5012, 5013, 5014, 5015})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupStudentCluster(nodes)
	time.Sleep(time.Second * WAIT_PERIOD) // Long enough for timeout+election

	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}
	leaderTerm := leader.GetCurrentTerm()

	// Partition off leader first node
	leader.NetworkPolicy.PauseWorld(true)

	followers := make([]*RaftNode, 0)
	for _, node := range nodes {
		if node != leader {
			followers = append(followers, node)
		}
	}

	// Partition second, third node into their own cluster, leaves fourth and fifth in their own cluster as well
	followers[0].NetworkPolicy.RegisterPolicy(*followers[0].GetRemoteSelf(), *followers[2].GetRemoteSelf(), false)
	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].GetRemoteSelf(), *followers[2].GetRemoteSelf(), false)
	followers[0].NetworkPolicy.RegisterPolicy(*followers[0].GetRemoteSelf(), *followers[3].GetRemoteSelf(), false)
	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].GetRemoteSelf(), *followers[3].GetRemoteSelf(), false)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].GetRemoteSelf(), *followers[0].GetRemoteSelf(), false)
	followers[3].NetworkPolicy.RegisterPolicy(*followers[3].GetRemoteSelf(), *followers[0].GetRemoteSelf(), false)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].GetRemoteSelf(), *followers[1].GetRemoteSelf(), false)
	followers[3].NetworkPolicy.RegisterPolicy(*followers[3].GetRemoteSelf(), *followers[1].GetRemoteSelf(), false)

	time.Sleep(time.Second * 2) // Wait, just in case the partitioned leader attempts anything

	// Test: Make sure that the original leader remains a leader post-partition
	if leader.State != LEADER_STATE {
		t.Errorf("Leader should remain leader even when partitioned off")
		return
	}

	// Test: None of the other nodes becomes a leader
	for _, node := range followers {
		if node.State == LEADER_STATE {
			t.Errorf("Node (%v) has become a leader - should remain a follower or be a candidate", node.Id)
		}
	}

	pausedLeaderTerm := leader.GetCurrentTerm()

	if leaderTerm != pausedLeaderTerm {
		t.Errorf("Leader's term should remain the same after partition. Went from %v to %v", leaderTerm, pausedLeaderTerm)
		return
	}

	// Test: add a new log entry to the leader - shouldn't be replicated
	leader.leaderMutex.Lock()
	logEntry := LogEntry{
		Index:  leader.getLastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.appendLogEntry(logEntry)
	leader.leaderMutex.Unlock()

	time.Sleep(time.Second * 2) // Finish waiting for cluster to settle

	// Bring the four followers back into one cluster
	followers[0].NetworkPolicy.RegisterPolicy(*followers[0].GetRemoteSelf(), *followers[2].GetRemoteSelf(), true)
	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].GetRemoteSelf(), *followers[2].GetRemoteSelf(), true)
	followers[0].NetworkPolicy.RegisterPolicy(*followers[0].GetRemoteSelf(), *followers[3].GetRemoteSelf(), true)
	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].GetRemoteSelf(), *followers[3].GetRemoteSelf(), true)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].GetRemoteSelf(), *followers[0].GetRemoteSelf(), true)
	followers[3].NetworkPolicy.RegisterPolicy(*followers[3].GetRemoteSelf(), *followers[0].GetRemoteSelf(), true)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].GetRemoteSelf(), *followers[1].GetRemoteSelf(), true)
	followers[3].NetworkPolicy.RegisterPolicy(*followers[3].GetRemoteSelf(), *followers[1].GetRemoteSelf(), true)

	time.Sleep(time.Second * WAIT_PERIOD * 2) // Wait for this cluster to elect a new leader

	newLeader, err := findLeader(followers)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	newLeader.leaderMutex.Lock()
	logEntry = LogEntry{
		Index:  newLeader.getLastLogIndex() + 1,
		TermId: newLeader.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{5, 6, 7, 8},
	}
	newLeader.appendLogEntry(logEntry)
	newLeader.leaderMutex.Unlock()

	time.Sleep(time.Second * WAIT_PERIOD) // Wait for log to get replicated

	// Bring the partitioned old leader back into the cluster
	leader.NetworkPolicy.PauseWorld(false)

	time.Sleep(time.Second * WAIT_PERIOD) // Finish waiting for cluster to settle

	// Test: Make sure that the leader becomes a follower, and the current leader remains the leader
	leaderNewState := leader.State
	if leaderNewState != FOLLOWER_STATE {
		t.Errorf("Old leader should fall back to the follower state after rejoining (was in %v state)", leaderNewState)
		return
	}

	newLeaderState := newLeader.State
	if newLeaderState != LEADER_STATE {
		t.Errorf("New leader should still be leader when old leader rejoins, but in %v state", newLeaderState)
		return
	}

}
