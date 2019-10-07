package our_tests

import (
	"testing"
	"time"

	"fmt"

	"github.com/stretchr/testify/assert"
)

func TestLeaderDropAndRestart(t *testing.T) {
	// leader drops out, no nodes have any log entries
	cluster, err := NewRaftCluster(t, DoubleElectionTimeoutConfig())
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	SetDebug(false)
	cluster.strictTests = false
	cluster.config.VerboseLog = true

	// election
	time.Sleep(cluster.GetConfig().ElectionTimeout * 4)

	// start client, execute some command
	client, _ := cluster.ClusterCmdClientRegister(nil)
	cluster.ClusterCmdClientCmdInit(client, "hello")
	cluster.ClusterCmdClientCmdHash(client)

	time.Sleep(cluster.GetConfig().ElectionTimeout * 4)

	leader, _ := cluster.GetSimpleLeader()
	cluster.ClusterCmdNodeShutdown(leader)
	Out.Printf("SHUTTING DOWN %v", leader)

	// re-election
	time.Sleep(time.Second * 5)

	Out.Printf("NEW LEADER ELECTED\n")

	// client needs to reconnect
	client, _ = cluster.ClusterCmdClientRegister(nil)
	time.Sleep(time.Second)
	Out.Printf("CLIENT CONNECTED\n")

	// send sequence of hashes w/o init and it should work
	for i := 0; i < 3; i++ {
		cluster.ClusterCmdClientCmdHash(client)
	}
	Out.Printf("CLIENT HASHES COMPLETED\n")
	time.Sleep(2)
}

func TestLeaderDropWithLargeLog(t *testing.T) {
	run := false
	if run {
		conf := SystemTestingConfig()
		conf.ClusterSize = 5
		cluster, err := NewRaftCluster(t, conf)
		cluster.strictTests = false
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		time.Sleep(cluster.GetConfig().ElectionTimeout * 10)
		leader, exists := cluster.GetLeader()
		if !exists {
			t.Errorf("No leader!")
			return
		}
		cluster.ClusterCmdClientStart()
		time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

		// some NON-LEADER nodes shutdown
		nonLeaders := make([]*RaftNode, 0)
		cluster.mutex.Lock()
		for _, node := range cluster.nodes {
			if node.State != LEADER_STATE {
				nonLeaders = append(nonLeaders, node)
			}
		}
		cluster.mutex.Unlock()
		cluster.ClusterCmdNodeShutdown(nonLeaders[0])
		cluster.ClusterCmdNodeShutdown(nonLeaders[1])
		cluster.ClusterCmdNodeShutdown(nonLeaders[2])

		time.Sleep(cluster.GetConfig().ElectionTimeout * 10)
		go cluster.ClusterCmdClientCmdHash(nil)
		go cluster.ClusterCmdClientCmdHash(nil)
		go cluster.ClusterCmdClientCmdHash(nil)
		time.Sleep(cluster.GetConfig().ElectionTimeout * 10)

		// leader drops, has uncommitted log entries b/c cannot commit to a majority of nodes
		cluster.ClusterCmdNodeShutdown(leader)

		// nodes elect new leader and receive commits
		time.Sleep(cluster.GetConfig().ElectionTimeout * 10)
		cluster.ClusterCmdNodeRestart(nonLeaders[0])
		cluster.ClusterCmdNodeRestart(nonLeaders[1])
		cluster.ClusterCmdNodeRestart(nonLeaders[2])
		time.Sleep(cluster.GetConfig().ElectionTimeout * 10)
		go cluster.ClusterCmdClientStart()
		go cluster.ClusterCmdClientCmdHash(nil)
		go cluster.ClusterCmdClientCmdHash(nil)

		// old leader starts up again and its log is truncated to be up to date with the new leader
		cluster.ClusterCmdNodeRestart(leader)
		// stable running resumes
		time.Sleep(cluster.GetConfig().ElectionTimeout * 10)
		cluster.assertLeaderState()
		// DESCRIBED IN DETAIL IN README
	}
}

func TestSplitVotes(t *testing.T) {
	// all nodes start as followers
	// all nodes timeout AT THE SAME TIME
	// every candidate should timeout and become a follower again
	// leader should elect normally

	cluster, _ := NewRaftCluster(t, DoubleElectionTimeoutConfig())

	for _, node := range cluster.nodes {
		cluster.initNode(node)
	}

	time.Sleep(time.Second * 2)
	// waits for all functions to be waiting on the lock and releases them all simultaneously
	Out.Printf("RELEASING ALL NODES INTO CANDIDATE STATE")
	cluster.debugCond.L.Lock()
	cluster.debugCond.Broadcast()
	cluster.debugCond.L.Unlock()

	// re-election
	time.Sleep(time.Second * 10)

	// must be able to find a leader
	leader, _ := cluster.GetSimpleLeader()
	Out.Printf("Cluster elected %v as leader", leader.Id)
}

func TestLeaderUpdatesFollowerWithPrevTerm(t *testing.T) {
	cluster, _ := NewRaftCluster(t, DefaultConfig())
	cluster.strictTests = false

	// election takes place
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	leader, _ := cluster.GetSimpleLeader()

	// followers both drop out
	for _, node := range cluster.nodes {
		if node.Id != leader.Id {
			cluster.ClusterCmdNodeShutdown(node)
		}
	}
	time.Sleep(time.Second * 3)

	// leader keeps going, increments term
	// adds entries to its log via client requests
	go cluster.ClusterCmdClientRegister(leader)
	time.Sleep(time.Second)
	go cluster.ClusterCmdClientRegister(leader)
	time.Sleep(time.Second)
	go cluster.ClusterCmdClientRegister(leader)
	time.Sleep(time.Second)

	lastTerm := leader.getLogEntry(leader.getLastLogIndex()).GetTermId()
	lastIndex := leader.getLastLogIndex()
	// followers restart and are updated to match leader's log
	for _, node := range cluster.nodes {
		if node.Id != leader.Id {
			cluster.ClusterCmdNodeRestart(node)
		}
	}
	time.Sleep(time.Second * 5)
	// check that all logs are consistent
	for _, node := range cluster.nodes {
		if node.Id != leader.Id {
			if node.getLogEntry(node.getLastLogIndex()).GetTermId() != lastTerm {
				t.Errorf("Last terms inconsistent")
			}
			if node.getLastLogIndex() != lastIndex {
				t.Errorf("Last index inconsistent")
			}
		}
	}
}

func TestLeaderUpdatesFollowerNextTermExtraEntries(t *testing.T) {
	conf := SystemTestingConfig()
	cluster, err := NewRaftCluster(t, conf)
	cluster.strictTests = false
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	// A follower, B leader with term 1
	B, isLeader := cluster.GetLeader()
	if !isLeader {
		fmt.Errorf("No leader!")
	}

	// B crashes and new election occurs
	cluster.ClusterCmdNodeShutdown(B)
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)
	// B restarts, all nodes in term 2
	cluster.ClusterCmdNodeRestart(B)
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	// A is leader, receives two request and crashes immediately w/o duplication
	A, isLeader := cluster.GetLeader()
	if !isLeader {
		fmt.Errorf("No leader!")
	}
	cl, err := cluster.ClusterCmdClientRegister(A)
	if err != nil {
		fmt.Errorf("Client failure!")
	}
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)
	aLogLen := A.getLastLogIndex() + 1
	go cluster.ClusterCmdClientCmdHash(cl)
	go cluster.ClusterCmdClientCmdHash(cl)
	go cluster.ClusterCmdClientCmdHash(cl)
	go cluster.ClusterCmdClientCmdHash(cl)
	for A.getLastLogIndex()+1 < aLogLen {
	}
	cluster.ClusterCmdNodeShutdown(A)

	// B is elected leader, in term 3
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)
	// A restarts, is now a follower with entries that are not in B's log
	cluster.ClusterCmdNodeRestart(A)
	// A should update correctly and B should remain leader
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)
	assert.Equal(t, LEADER_STATE, B.State)
}

// PARTITION TESTS
func TestSoloNodePartition(t *testing.T) {
	cluster, err := NewRaftCluster(t, SystemTestingConfig())
	cluster.strictTests = false
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	// single node breaks off into partition and keeps running, continuously going into the candidate state
	// and incrementing its turn
	rightSideNodes, leftSideNodes := cluster.ClusterCmdRandPartitionSplit(1)

	time.Sleep(cluster.GetConfig().ElectionTimeout * 30)

	// the right side's single node's term should be larger than everyone's
	// on the left at this point
	rightIncrementerTerm := rightSideNodes[0].GetCurrentTerm()
	for _, node := range leftSideNodes {
		assert.True(t, rightIncrementerTerm > node.GetCurrentTerm(),
			"rightIncrementerTerm=%d, curTerm=%d, node=%v", rightIncrementerTerm, node.GetCurrentTerm(), node)
	}

	// when partition is healed, all other nodes will have lower term but nobody will vote for the solo node
	// because log is out of date
	// all other nodes will be brought to the term of the solo node and solo node will become follower
	// THIS ALSO TESTS THAT OUT OF DATE LOGS CANNOT BE ELECTED
	cluster.ClusterCmdResolvePartition()

	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	// everyone's should be equal now
	rightIncrementerTerm = rightSideNodes[0].GetCurrentTerm()
	for _, node := range leftSideNodes {
		assert.Equal(t, rightIncrementerTerm, node.GetCurrentTerm())
	}

	// NOTE: look at assertLogEntries and assertLeaderState for how these are tested
}

func TestTwoLeaderPartition(t *testing.T) {
	// RAFT HAS 5 NODES IN THIS TEST - A,B,C,D,E
	conf := SystemTestingConfig()
	conf.ClusterSize = 5

	cluster, err := NewRaftCluster(t, conf)
	cluster.strictTests = false
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	// A (without loss of generality) is elected leader, partition is setup between A,B|C,D,E
	rightSideNodes, leftSideNodes := cluster.ClusterCmdRandPartitionSplit(2)
	time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

	// A stays leader of its side of the partition, C,D,E reelect and become a greater term
	// We have two leaders on both sides of the partition
	var A = new(RaftNode)
	for _, node := range rightSideNodes {
		if node.State == LEADER_STATE {
			A = node
		}
	}
	if A == nil {
		t.Errorf("No leader election on side of A!")
	} else {
		// Add another client to the new side of the partition and update both sides with a different call
		leftClientEntry := cluster.GetRandomNodeFrom(leftSideNodes)
		cl, err := cluster.ClusterCmdClientRegister(leftClientEntry)
		if err != nil {
			t.Errorf("Couldn't create client on CDE side")
			return
		}
		cluster.ClusterCmdClientCmdInit(cl, "cats")
		cluster.ClusterCmdClientCmdHash(cl)
		cluster.ClusterCmdClientCmdHash(cl)
		cluster.ClusterCmdClientCmdHash(cl)
		cluster.ClusterCmdClientCmdHash(cl)
		cluster.ClusterCmdClientCmdHash(cl)

		time.Sleep(cluster.GetConfig().ElectionTimeout * 5)

		// Remove partition, A should step down (as it has no committed entries) and match C,D,E's log
		cluster.ClusterCmdResolvePartition()
		time.Sleep(cluster.GetConfig().ElectionTimeout * 5)
		assert.True(t, A.State != LEADER_STATE)
	}
}
