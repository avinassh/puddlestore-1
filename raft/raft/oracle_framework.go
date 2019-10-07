package raft

import (
	"errors"
	"math/rand"
	"time"

	"testing"

	"fmt"

	"os"

	"sync"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/raft/hashmachine"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/tapestry/tapestry"
	"github.com/stretchr/testify/assert"
)

/**************************************************************************************/
// Init
/**************************************************************************************/

// container for testing cluster states
type RaftCluster struct {
	t *testing.T

	nodes                []*RaftNode // all nodes, including shutdown
	shutdownNodes        []*RaftNode
	clients              []*Client
	config               *Config
	mutex                sync.Mutex
	debug                chan NodeState
	debugMutex           sync.Mutex
	debugCond            *sync.Cond
	hashChainInitialized bool
	isPartitioned        bool

	// config
	strictTests bool // on to run tests that will succeed MOST but not all of the time
}

// creates new, fully connected raft cluster based on config
func NewRaftCluster(t *testing.T, config *Config) (rc *RaftCluster, err error) {
	SetDebug(true)

	if rmErr := os.RemoveAll("raftlogs/"); rmErr != nil {
		fmt.Errorf("could not remove raftlogs; continuing: %v", rmErr)
	}
	if config.ClusterSize <= 1 {
		return nil, fmt.Errorf("cluster size too small")
	}

	rc = new(RaftCluster)
	rc.t = t
	rc.nodes = make([]*RaftNode, config.ClusterSize)
	rc.clients = make([]*Client, 0)
	rc.config = config
	rc.debug = make(chan NodeState, 10000)
	rc.debugCond = sync.NewCond(&rc.debugMutex)
	rc.hashChainInitialized = false

	// default config values
	rc.strictTests = true

	// seed random
	rand.Seed(time.Now().UTC().UnixNano())

	startNode, err := CreateNode(0, nil, config)
	if err != nil {
		t.Errorf("could not create starting startNode")
		return nil, err
	}

	rc.nodes[0] = startNode
	for i := 1; i < config.ClusterSize; i++ {
		time.Sleep(time.Millisecond * 500)
		node, err := CreateNode(0, startNode.GetRemoteSelf(), config) // rand port
		if err != nil {
			t.Errorf("error creating startNode %d: %v", i, err)
			return nil, err
		}
		rc.nodes[i] = node
	}

	// busy loop until all nodes are not in join_state
	for {
		done := true
		for _, node := range rc.nodes {
			if node.State == JOIN_STATE {
				done = false
				break
			}
		}
		if done {
			return
		}
	}
	return
}

func (rc *RaftCluster) initNode(node *RaftNode) {
	node.debugCond = rc.debugCond
	node.debugMutex = rc.debugMutex
}

/**************************************************************************************/
// Helpers
/**************************************************************************************/
func (rc *RaftCluster) GetLeader() (leader *RaftNode, present bool) {
	_, present, leader = rc.assertLeaderState()
	return
}

// Get any non-leader node. Not guaranteed to be random
func (rc *RaftCluster) GetNonLeader() *RaftNode {
	rc.assertLeaderState()
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	for _, node := range rc.nodes {
		if node.State != LEADER_STATE {
			return node
		}
	}
	return nil // no non-leader!
}

func (rc *RaftCluster) GetConfig() *Config {
	return rc.config
}

func (rc *RaftCluster) Printf(formatString string, args ...interface{}) {
	Out.Printf("ORACLE: %v", fmt.Sprintf(formatString, args...))
}

func (rc *RaftCluster) GetRandomNodeFrom(nodes []*RaftNode) *RaftNode {
	if len(nodes) > 0 {
		return nodes[rand.Intn(len(nodes))]
	}
	return nil
}

func (rc *RaftCluster) GetRandomNode() *RaftNode {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	return rc.GetRandomNodeFrom(rc.nodes)
}

func (rc *RaftCluster) GetRandomShutdownNode() *RaftNode {
	return rc.GetRandomNodeFrom(rc.shutdownNodes)
}

func (rc *RaftCluster) GetRandomLiveNode() *RaftNode {
	node := rc.GetRandomNode()
	i := 1
	maxTries := 3 * len(rc.nodes)
	for node.IsShutdown && i < maxTries {
		node = rc.GetRandomNode()
		// make sure they're in the shutdown list
		rc.mutex.Lock()
		if res, _ := ContainsNode(rc.shutdownNodes, node); res {
			rc.shutdownNodes = append(rc.shutdownNodes, node)
		}
		rc.mutex.Unlock()
		i++
	}
	if i == maxTries {
		// try and get one in the unlikely chance we missed all
		for _, node := range rc.nodes {
			if !node.IsShutdown {
				return node
			}
		}
		return nil
	}
	return node
}

func ContainsNode(nodes []*RaftNode, node *RaftNode) (res bool, i int) {
	for i, n := range nodes {
		if node.Id == n.Id {
			return true, i
		}
	}
	return false, -1
}

func (rc RaftCluster) GetRaftNodeByRemote(rem *RemoteNode) *RaftNode {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()
	for i := range rc.nodes {
		thisRem := rc.nodes[i].GetRemoteSelf()
		// clients may not have the Id field
		if thisRem.Id == rem.Id || thisRem.Addr == rem.Addr {
			return rc.nodes[i]
		}
	}
	return nil
}

func (rc *RaftCluster) GetRandomClient() (cl *Client, i int) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	if len(rc.clients) > 0 {
		i = rand.Intn(len(rc.clients))
		return rc.clients[i], i
	}
	return nil, -1
}

func withProb(prob float64) bool {
	return rand.Float64() <= prob
}

// Returns the log elements of the given node up to the given index
func getLogUpTo(node *RaftNode, index uint64) []*LogEntry {
	log := make([]*LogEntry, 0)
	for i := uint64(0); i < index; i++ {
		log = append(log, node.getLogEntry(i))
	}
	return log
}

// checks that all non-shutdown nodes currently have the same leader, and return that leader if so
// SUBJECT TO RACE CONDITIONS AND IF YOU CALL IT IN A TRANSITION TIME
func (rc *RaftCluster) GetSimpleLeader() (rn *RaftNode, err error) {
	defer rc.assertNodeLogs() // put this everywhere!

	var leader *RemoteNode = nil

	// prints error
	defer func() {
		if err != nil {
			rc.t.Errorf(err.Error())
		}
	}()

	// hangs until every node has a valid .leader field --> NOT THREADSAFE
	for {
		election := false
		rc.mutex.Lock()
		for _, node := range rc.nodes {
			if node.Leader == nil {
				election = true
				break
			}
		}
		rc.mutex.Unlock()
		if !election {
			break
		}
		time.Sleep(rc.GetConfig().ElectionTimeout * 2)
	}

	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	for _, node := range rc.nodes {
		if !node.IsShutdown {
			if leader == nil {
				leader = node.Leader
			} else if node.Leader.GetAddr() != leader.GetAddr() {
				err := errors.New("inconsistent leader across nodes")
				rc.t.Error(err.Error())
				return nil, err
			}
		}
	}

	// find the node with that given address
	for i, node := range rc.nodes {
		//Out.Printf("COMPARING %v:%v and %v", node.GetRemoteSelf(), node.Leader, leader)
		if !node.IsShutdown && node.GetRemoteSelf().Addr == leader.Addr {
			return rc.nodes[i], nil
		}
	}

	err = errors.New("no node in range matched leader address")
	rc.t.Error(err.Error())
	return nil, err
}

/**************************************************************************************/
// Node Commands
/**************************************************************************************/

// ClusterCmdNodeShutdown gracefully exits the given node. Randomly chooses a node if none assigned
func (rc *RaftCluster) ClusterCmdNodeShutdown(node *RaftNode) bool {
	defer rc.assertNodeLogs() // put this everywhere!

	if node == nil {
		node = rc.GetRandomLiveNode()
	}
	// never remove all of them to avoid getting locked down
	if len(rc.shutdownNodes) >= rc.config.ClusterSize-1 {
		rc.Printf("Stopped shutting down nodes because only one left!")
		return false
	}

	rc.Printf("Shutting down node %v (down: %d/%d)", node.GetRemoteSelf(), len(rc.shutdownNodes), rc.config.ClusterSize)
	if !node.IsShutdown {
		node.GracefulExit()
	}
	// make sure they're in the shutdown list
	rc.mutex.Lock()
	if in, _ := ContainsNode(rc.shutdownNodes, node); !in {
		rc.shutdownNodes = append(rc.shutdownNodes, node)
	}
	rc.mutex.Unlock()
	return true
}

// ClusterCmdNodeRestart tries to resume the given node if it's shutdown,
// or a random shutdown node if given null and one exists
func (rc *RaftCluster) ClusterCmdNodeRestart(prevNode *RaftNode) (bool, *RaftNode) {
	defer rc.assertNodeLogs() // put this everywhere!

	if prevNode == nil {
		prevNode = rc.GetRandomShutdownNode()
		if prevNode == nil {
			rc.Printf("No nodes shutdown that can re-enter")
			return false, nil
		}
	}
	rc.mutex.Lock()
	if res, i := ContainsNode(rc.shutdownNodes, prevNode); res {
		// remove from shutdown nodes
		rc.shutdownNodes = append(rc.shutdownNodes[:i], rc.shutdownNodes[i+1:]...)
	}
	rc.mutex.Unlock()

	// make sure prevNode is fully shut down
	prevNode.server.Stop()
	time.Sleep(100 * time.Millisecond)

	// create new node from scratch at the same port so it uses the same logs
	connectPoint := rc.GetRandomLiveNode()
	if connectPoint == nil {
		errs := "No nodes available to connect to restart node..."
		rc.Printf(errs)
		rc.t.Errorf(errs)
		return false, nil
	}
	newNode, err := CreateNode(prevNode.port, connectPoint.GetRemoteSelf(), rc.config)
	if err == nil && newNode != nil {
		rc.Printf("Restarting node %v as %v (down: %d/%d)",
			prevNode.GetRemoteSelf(), newNode.GetRemoteSelf(), len(rc.shutdownNodes), rc.config.ClusterSize)
		rc.mutex.Lock()
		present, prevI := ContainsNode(rc.nodes, prevNode)
		assert.True(rc.t, present)
		rc.nodes[prevI] = newNode
		rc.mutex.Unlock()

		// wait for surety before assertions (some reasonable amount of time
		startTime := time.Now()
		for {
			if newNode.State == JOIN_STATE || time.Since(startTime) > (5*rc.config.ElectionTimeout) {
				break
			}
			rc.Printf("Waiting on node resume......")
			time.Sleep(25 * time.Millisecond)
		}
	} else {
		rc.t.Errorf(err.Error())
		return false, nil
	}
	return true, newNode
}

/**************************************************************************************/
// Network Policy Commands
/**************************************************************************************/

// partitions a single node from the cluster
func (rc *RaftCluster) ClusterCmdRandNodeSever() {
	node := rc.GetRandomLiveNode()
	if node != nil {
		rc.Printf("Severing node %v from rest", node.GetRemoteSelf())
		node.NetworkPolicy.PauseWorld(true)
	}
	rc.assertNodeLogs() // put this everywhere!
}

// semi-randomly partitions the set of all nodes with numOnOneSide on one "side",
// trying to keep more than one node on each side if possible
func (rc *RaftCluster) ClusterCmdRandPartitionSplit(numOnOneSide int) (rightSideNodes []*RaftNode, leftSideNodes []*RaftNode) {
	rc.Printf("Partitioning with %d on one side", numOnOneSide)

	numOnRightSide := rand.Intn(rc.config.ClusterSize / 2)
	if numOnOneSide > rc.config.ClusterSize {
		rc.t.Errorf("Invalid numOnOneSide argument to ClusterCmdRandPartitionSplit: %d", numOnOneSide)
		return nil, nil
	} else if numOnOneSide != 0 {
		numOnRightSide = numOnOneSide
	}

	rightSideNodes = make([]*RaftNode, 0)
	for len(rightSideNodes) < numOnRightSide {
		candidate := rc.GetRandomNode()
		if res, _ := ContainsNode(rightSideNodes, candidate); !res {
			rightSideNodes = append(rightSideNodes, candidate)
		}
	}

	leftSideNodes = make([]*RaftNode, 0)
	rc.mutex.Lock()
	for _, node := range rc.nodes {
		// only take ones not on right side
		if res, _ := ContainsNode(rightSideNodes, node); !res {
			leftSideNodes = append(leftSideNodes, node)
		}
	}
	rc.mutex.Unlock()

	// block all nodes on each side from all nodes on other
	rc.isPartitioned = true
	for _, lnode := range leftSideNodes {
		for _, rnode := range rightSideNodes {
			// only set outgoing because it's all that matters
			lnode.NetworkPolicy.RegisterPolicy(*lnode.GetRemoteSelf(), *rnode.GetRemoteSelf(), false)
			rnode.NetworkPolicy.RegisterPolicy(*rnode.GetRemoteSelf(), *lnode.GetRemoteSelf(), false)
		}
	}

	// test test
	assert.Equal(rc.t, rc.config.ClusterSize, len(leftSideNodes)+len(rightSideNodes),
		"numOnRightSide=%d, len(right)=%d, len(left)=%d", numOnRightSide, len(rightSideNodes), len(leftSideNodes))
	rc.assertNodeLogs() // put this everywhere!

	return rightSideNodes, leftSideNodes
}

// random version of ClusterCmdRandPartitionSplit
func (rc *RaftCluster) ClusterCmdRandPartitionSplitRand() {
	rc.ClusterCmdRandPartitionSplit(rand.Intn(len(rc.nodes)))
}

// randomly chooses approximately percentConnections (from [0.0 to 1.0] ) out of all possible
// (directed, non-self) connections, and blocks (severs) that subset
func (rc *RaftCluster) ClusterCmdRandPartition(percentConnections float64) {
	rc.mutex.Lock()

	rc.Printf("Partitioning randomly")
	rc.isPartitioned = true
	for _, node1 := range rc.nodes {
		for _, node2 := range rc.nodes {
			// ignore self-connections because they have no effect
			if node1.Id != node2.Id {
				// with percentConnections probability, sever connection from node1 to node2
				// (note we cover both directions in this loop)
				if withProb(percentConnections) {
					node1.NetworkPolicy.RegisterPolicy(*node1.GetRemoteSelf(), *node2.GetRemoteSelf(), false)
				}
			}
		}
	}
	rc.mutex.Unlock()
	rc.assertNodeLogs() // put this everywhere!
}

// random version of ClusterCmdRandPartition
func (rc *RaftCluster) ClusterCmdRandPartitionRand() {
	rc.ClusterCmdRandPartition(rand.Float64())
}

// removes any and all network partitions
func (rc *RaftCluster) ClusterCmdResolvePartition() {
	rc.mutex.Lock()

	rc.Printf("Removing partitions")
	for _, node := range rc.nodes {
		node.NetworkPolicy.ClearPolicies()
	}
	rc.mutex.Unlock()
	rc.isPartitioned = false
	rc.assertNodeLogs() // put this everywhere!
}

/**************************************************************************************/
// Client Commands
/**************************************************************************************/
func (rc *RaftCluster) ClusterCmdClientStart() (cl *Client, err error) {
	cl, err = rc.ClusterCmdClientRegister(nil)
	if cl != nil && err == nil && !rc.hashChainInitialized {
		err = rc.ClusterCmdClientCmdInit(cl, tapestry.RandString(5))
	}
	rc.assertNodeLogs() // put this everywhere!
	return cl, err
}

func (rc *RaftCluster) ClusterCmdClientRegister(node *RaftNode) (cl *Client, err error) {
	if node == nil {
		node = rc.GetRandomLiveNode()
		if node == nil {
			rc.Printf("Register client: no live nodes to connect to")
			return
		}
	}

	cl, err = Connect(node.GetRemoteSelf().Addr)
	rc.mutex.Lock()
	rc.clients = append(rc.clients, cl)
	rc.mutex.Unlock()
	rc.Printf("Registered client %v (clients: %v)", cl, rc.clients)
	if rc.shouldClientRequestSucceed(cl) && err != nil {
		rc.t.Errorf("%v: could't connect client", err.Error())
	}
	rc.assertNodeLogs() // put this everywhere!
	return
}

// Initiates the hash chain with the string init from the client cl
func (rc *RaftCluster) ClusterCmdClientCmdInit(cl *Client, init string) error {
	if cl == nil {
		cl, _ = rc.GetRandomClient()
		if cl == nil {
			return fmt.Errorf("No clients available and none provided")
		}
	}

	// wait for the client to finish initializing if it isn't
	for cl.Leader == nil {
		time.Sleep(10 * time.Millisecond)
	}

	rc.Printf("Sending state machine command (init)")
	// tentatively say initiated
	rc.hashChainInitialized = true
	_, err := cl.SendRequest(hashmachine.HASH_CHAIN_INIT, []byte(init))
	if rc.shouldClientRequestSucceed(cl) && err != nil {
		rc.t.Errorf("%v: could not perform client init", err.Error())
		// rescind initiated
		rc.hashChainInitialized = false
	}
	rc.assertNodeLogs() // put this everywhere!
	return err
}

func (rc *RaftCluster) ClusterCmdClientCmdHash(cl *Client) error {
	if cl == nil {
		cl, _ = rc.GetRandomClient()
		if cl == nil {
			return fmt.Errorf("No clients available and none provided")
		}
	}

	// generate random bytes
	n := rand.Intn(20)
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = byte(rand.Int())
	}

	// wait for the client to finish initializing if it isn't
	for cl.Leader == nil {
		time.Sleep(10 * time.Millisecond)
	}

	rc.Printf("Sending state machine command (hash)")
	_, err := cl.SendRequest(hashmachine.HASH_CHAIN_ADD, bytes)
	if rc.shouldClientRequestSucceed(cl) && err != nil {
		if rc.strictTests {
			rc.t.Errorf("could not perform client hash: %v", err.Error())
		}
	}
	rc.assertNodeLogs() // put this everywhere!
	return err
}

func (rc *RaftCluster) ClusterCmdClientCmdRand() (cl *Client, err error) {
	cl, _ = rc.GetRandomClient()
	if cl == nil {
		err = fmt.Errorf("command sent with no clients registered: %v", rc.clients)
		rc.Printf("%v", err)
		return nil, err
	} else if !rc.hashChainInitialized {
		err = rc.ClusterCmdClientCmdInit(cl, tapestry.RandString(5))
	} else {
		err = rc.ClusterCmdClientCmdHash(cl)
	}
	return
}

func (rc *RaftCluster) ClusterCmdClientLeave() {
	// no special interaction with client; just remove from list
	cl, i := rc.GetRandomClient()
	if cl != nil {
		rc.Printf("Removed client %v (clients: %v)", cl, rc.clients)
		rc.mutex.Lock()
		rc.clients = append(rc.clients[:i], rc.clients[i+1:]...)
		rc.mutex.Unlock()
	}
	rc.assertNodeLogs() // put this everywhere!
}

/**************************************************************************************/
// Tests / Assertions on cluster
/**************************************************************************************/
func (rc *RaftCluster) shouldClientRequestSucceed(client *Client) bool {
	if client.Leader == nil {
		return false
	}
	leaderNode := rc.GetRaftNodeByRemote(client.Leader)
	if leaderNode == nil {
		rc.t.Errorf("Client had reference to leader (%v) that doesn't exist (nodes: %v)!", client.Leader, rc.nodes)
		return false
	} else {
		return !leaderNode.IsShutdown
	}
}

// asserts that all followers logs are consistent to the extent they can be:
//  - all entries that are committed are the same, or at least replicated on a majority
//  - if leader is present, make assertions about their log vs. the followers
// note shutdown state doesn't matter here
func (rc *RaftCluster) assertNodeLogs() (valid bool) {
	// get leader commit index to see what part of the log we expect to be
	// replicated on a majority
	_, _, leader := rc.assertLeaderState()

	commitIndex := uint64(0)
	highestLogTerm := uint64(0)
	committedLeaderLog := make([]*LogEntry, 0)

	// if the leader doesn't exist, take the smallest commitIndex/log to do some comparison
	if leader == nil {
		minCommitIndex := uint64(rc.nodes[0].commitIndex)
		committedLeaderLog = getLogUpTo(rc.nodes[0], minCommitIndex)
		rc.mutex.Lock()
		for _, node := range rc.nodes {
			// take info from one with min commit index
			if node.commitIndex < minCommitIndex {
				minCommitIndex = node.commitIndex
				committedLeaderLog = getLogUpTo(node, minCommitIndex)
			}

			// just set highestLogTerm to highest globally... so no error possible
			lastLogEntry := node.getLogEntry(node.getLastLogIndex())
			if lastLogEntry != nil && lastLogEntry.TermId > highestLogTerm {
				highestLogTerm = lastLogEntry.TermId
			}
		}
		rc.mutex.Unlock()
		commitIndex = minCommitIndex

	} else {
		// grab this here (essentially only working with the snapshot at this moment)
		// i.e. some logs may have their commit indexes incremented as we're here
		leader.leaderMutex.Lock()
		commitIndex = leader.commitIndex
		highestLogTerm = leader.getLogEntry(leader.getLastLogIndex()).TermId
		committedLeaderLog = getLogUpTo(leader, commitIndex)
		leader.leaderMutex.Unlock()
	}

	// make sure at least a majority satisfies the log equality part
	numLogsEqual := 0

	// then, confirm that a majority of logs match below that commit index
	// note followers may be candidates but they still should have valid logs
	rc.mutex.Lock()
	for _, follower := range rc.nodes {
		// can only make this assertion if we had a leader, and are not the leader
		// (it may update it)
		// (watch out for split leaders)
		if leader != nil && follower.Id != leader.Id && follower.State != LEADER_STATE {
			assert.True(rc.t, follower.commitIndex <= commitIndex,
				"follower=%v, followerCommitIndex=%d, commitIndex=%d, leader=%v", follower, follower.commitIndex, commitIndex, leader)
		}

		follower.leaderMutex.Lock()
		committedFollowerLog := getLogUpTo(follower, min(commitIndex, follower.getLastLogIndex()+1))

		// check for log equality / majority equality
		equal := assert.ObjectsAreEqual(committedLeaderLog, committedFollowerLog)
		if rc.strictTests {
			assert.Equal(rc.t, committedLeaderLog, committedFollowerLog, "STRICT TEST; OKAY TO FAIL | commitIndex=%d, numLogsEqual=%d (so far)", commitIndex, numLogsEqual)
		}
		if equal {
			numLogsEqual++
		}

		// also, if there is currently a leader, make sure that they have the
		// highest term in their log
		// watchout for split leaders
		if follower.State != LEADER_STATE {
			followerLastEntry := follower.getLogEntry(follower.getLastLogIndex())
			assert.True(rc.t, followerLastEntry == nil || followerLastEntry.TermId <= highestLogTerm,
				"follower=%v, lastEntry=%v, highestLogTerm=%d", follower, followerLastEntry, highestLogTerm)
		}

		follower.leaderMutex.Unlock()
	}
	rc.mutex.Unlock()

	assert.True(rc.t, float64(numLogsEqual)/float64(rc.config.ClusterSize) > 0.5,
		"numLogsEqual=%d, leader=%v, commitIndex=%d, committedLeaderLog=%v", numLogsEqual, leader, commitIndex, committedLeaderLog)

	return true
}

// asserts that the cluster either has a valid leader or is in
func (rc *RaftCluster) assertLeaderState() (valid bool, present bool, leader *RaftNode) {
	numLeaders := 0
	numSameTermLeaders := 0
	highestLeaderTerm := uint64(0)
	highestFollowerTerm := uint64(0)
	leader = nil
	numCandidates := 0
	numExitedNodes := 0

	rc.mutex.Lock()
	for _, node := range rc.nodes {
		if node.IsShutdown { // ignore shutdown
			numExitedNodes++
		} else if node.State == LEADER_STATE {
			numLeaders++
			nodeTerm := node.GetCurrentTerm()
			if nodeTerm > highestLeaderTerm { // note leader term must be >= 1
				leader = node // choose random if many
				highestLeaderTerm = nodeTerm
				numSameTermLeaders = 1
			} else if nodeTerm == highestLeaderTerm {
				if leader == nil {
					leader = node
				} else {
					// check for the more likely one to win
					leaderLastElm := node.getLogEntry(node.getLastLogIndex())
					if leaderLastElm != nil && node.logMoreUpdatedThan(leader.getLastLogIndex(), leaderLastElm.TermId) {
						leader = node
					}
				}
				numSameTermLeaders++
			}
		} else if node.State == CANDIDATE_STATE {
			numCandidates++
		} else if node.State == FOLLOWER_STATE {
			if node.GetCurrentTerm() > highestFollowerTerm {
				highestFollowerTerm = node.GetCurrentTerm()
			}
		}
	}
	rc.mutex.Unlock()

	if rc.strictTests {
		assert.Equal(rc.t, 1, numLeaders)
		assert.Equal(rc.t, 1, numSameTermLeaders)
		assert.True(rc.t, highestFollowerTerm <= highestLeaderTerm)
	}
	validCond := 1 == numSameTermLeaders ||
		(numSameTermLeaders == 0 &&
			(numCandidates > 0 || numExitedNodes > 0)) ||
		(numSameTermLeaders > 1 && rc.isPartitioned)

	assert.True(rc.t, validCond,
		"numSameTermLeaders=%d, numCandidates=%d, numExitedNodes=%d, isPartitioned",
		numSameTermLeaders, numCandidates, numExitedNodes, rc.isPartitioned)

	return validCond, numSameTermLeaders > 0, leader
}
