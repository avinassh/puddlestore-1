package tapestry

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Tests that published objects show up on the root for that object
func TestPublishOnce(t *testing.T) {
	nodes, _ := CreateTapestryNetworkRand(t, 2)
	if nodes == nil {
		// error already created
		return
	}

	key := "goose"
	assert.False(t, IsPublished(t, key, nodes[0], nodes))

	nodes[0].publishOnce(key, Hash(key))
	assert.True(t, IsPublished(t, key, nodes[0], nodes))
}

// Tests that objects are periodically published
func TestPeriodicallyPublish(t *testing.T) {

	// immediate kill
	nodes, _ := CreateTapestryNetworkRand(t, 2)
	if nodes == nil {
		// error already created
		return
	}

	key := "cat"
	assert.False(t, IsPublished(t, key, nodes[0], nodes))

	done := make(chan bool)
	go nodes[0].periodicallyPublish(key, Hash(key), done)
	assert.False(t, IsPublished(t, key, nodes[0], nodes))

	// immediately say done before first publish (we assume it's a reasonable delay)
	done <- true
	assert.False(t, IsPublished(t, key, nodes[0], nodes))

	key = "dog"
	done = make(chan bool)
	go nodes[1].periodicallyPublish(key, Hash(key), done)
	assert.False(t, IsPublished(t, key, nodes[1], nodes))

	WaitUntilPublished(t, key, nodes[1], nodes)

	dur := TIMEOUT + TIMINGS_BUFFER
	Out.Printf("Waiting %v for at least one timeout...", dur)
	time.Sleep(dur)
	assert.True(t, IsPublished(t, key, nodes[1], nodes))

	done <- true
	unpublished := time.Now()
	assert.True(t, IsPublished(t, key, nodes[1], nodes)) // Should still be published
	Out.Printf("Waiting for publish to time out...")
	for IsPublished(t, key, nodes[1], nodes) {
		time.Sleep(PUBLISH_POLL_DUR)
	}
	assert.InDelta(t, TIMEOUT, time.Since(unpublished), float64(TIMINGS_BUFFER))
	assert.False(t, IsPublished(t, key, nodes[1], nodes))
}

func TestFindRootSingleNode(t *testing.T) {
	if DIGITS == 4 && BASE == 16 {

		nodes := CreateTapestryNetwork(t, []ID{{3, 4, 5, 6}})
		if nodes == nil {
			// error already created
			return
		}

		root, err, hops, _ := nodes[0].findRoot(nodes[0].node, ID{3, 4, 5, 6})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{3, 4, 5, 7})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{0, 0, 0, 0})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{5, 8, 10, 12})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{15, 15, 15, 15})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

	}
}

func TestFindRootTwoNodes(t *testing.T) {
	if DIGITS == 4 && BASE >= 16 {
		nodes := CreateTapestryNetwork(t,
			[]ID{
				{3, 4, 5, 6},
				{15, 12, 3, 9}})
		if nodes == nil {
			// error already created
			return
		}

		// to 1
		root, err, hops, _ := nodes[0].findRoot(nodes[0].node, ID{3, 4, 5, 6})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[1].node, ID{3, 4, 5, 6})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 2, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{2, 5, 3, 6})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[1].findRoot(nodes[1].node, ID{2, 5, 3, 6})
		assert.Nil(t, err)
		assert.Equal(t, nodes[0].node, root)
		assert.Equal(t, 2, hops)

		// to 2
		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{15, 12, 3, 9})
		assert.Nil(t, err)
		assert.Equal(t, nodes[1].node, root)
		assert.Equal(t, 2, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[1].node, ID{15, 12, 3, 9})
		assert.Nil(t, err)
		assert.Equal(t, nodes[1].node, root)
		assert.Equal(t, 1, hops)

		root, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{12, 13, 1, 7})
		assert.Nil(t, err)
		assert.Equal(t, nodes[1].node, root)
		assert.Equal(t, 2, hops)

		root, err, hops, _ = nodes[1].findRoot(nodes[0].node, ID{15, 12, 3, 8})
		assert.Nil(t, err)
		assert.Equal(t, nodes[1].node, root)
		assert.Equal(t, 2, hops)

		// 2nd fails
		nodes[1].Kill()
		WaitUntilUnresponsive(nodes[1])

		// start from 2nd
		_, err, hops, _ = nodes[0].findRoot(nodes[1].node, ID{15, 12, 3, 8})
		assert.NotEqual(t, nil, err)
		assert.Equal(t, 1, hops) // can't find anything
		// start from 1st
		_, err, hops, _ = nodes[0].findRoot(nodes[0].node, ID{15, 12, 3, 8})
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, hops)

	} else {
		t.Errorf("Incorrect DIGIT and BASE setting (need DIGIT = 4, BASE > 16)")
	}
}

// returns expected root for routing to idTo in a network of nodeIDs
func expectedRoot(idTo ID, nodes []*Node) (node *Node) {
	bestChoiceNode := nodes[0]
	for _, node := range nodes {
		if idTo.BetterChoice(node.node.Id, bestChoiceNode.node.Id) {
			bestChoiceNode = node
		}
	}
	return bestChoiceNode
}

func TestFindRootManyNodes(t *testing.T) {
	const NETWORK_SIZE = 20
	const NUM_RAND_ID_TRIES = 150
	SetDebug(true)

	nodes, ids := CreateTapestryNetworkRand(t, NETWORK_SIZE)
	if nodes == nil {
		// error already created
		return
	}
	Out.Printf("Nodes: %v", nodes)

	// try every possible route between and check we're okay
	for i := 0; i < NETWORK_SIZE; i++ {
		for j := 0; j < NETWORK_SIZE; j++ {
			from := nodes[i]
			to := nodes[j]
			start, _, _ := RandNode(nodes)
			root, err, numhops, hops := from.findRoot(start.node, to.node.Id)
			expected := expectedRoot(to.node.Id, nodes)

			testPass := true
			testPass = testPass && assert.Nil(t, err)
			testPass = testPass && assert.Equal(t, expected.node, root,
				"Routing from %v to %v, starting at %v (Nodes: %v)\n",
				from.node, to.node, start.node, ids)
			testPass = testPass && assert.True(t, numhops <= NETWORK_SIZE)
			if !testPass {
				Debug.Printf("expected: %v, actual: %v (numhops: %v)", expected.node, root, numhops)
				PrintRoute(start.node, to.node.Id, hops, nodes)
			}
		}
	}

	// also try random inputs
	for i := 0; i < NUM_RAND_ID_TRIES; i++ {
		from, _, _ := RandNode(nodes)
		start, _, _ := RandNode(nodes)
		idTo := RandomID()
		root, err, numhops, hops := from.findRoot(start.node, idTo)
		expected := expectedRoot(idTo, nodes)

		testPass := true
		testPass = testPass && assert.Nil(t, err)
		testPass = testPass && assert.Equal(t, expected.node, root,
			"Routing from %v to %v, starting at %v (Nodes: %v)",
			from.node, idTo, start.node, ids)
		testPass = testPass && assert.True(t, numhops <= NETWORK_SIZE)
		if !testPass {
			Debug.Printf("expected: %v, actual: %v (numhops: %v)", expected.node, root, numhops)
			PrintRoute(start.node, idTo, hops, nodes)
		}
	}

}

// Tests either failures while finding root, either by using KILL or LEAVE
// as specified (see tests that use this)
func findRootFailures(t *testing.T, graceful bool) {

	const NETWORK_SIZE = 10
	const REMOVE_PROB = 0.4

	nodes, ids := CreateTapestryNetworkRand(t, NETWORK_SIZE)
	if nodes == nil {
		// error already created
		return
	}

	// random drop outs
	for len(nodes) > 3 { // need 2 for routing
		lenBefore := len(nodes)
		// remove one at random
		var killed *Node = nil
		if rand.Float64() < REMOVE_PROB {
			remI := rand.Intn(len(nodes))
			killed = nodes[remI]
			if graceful {
				killed.Leave()
			} else {
				killed.Kill()
			}

			nodes = append(nodes[:remI], nodes[remI+1:]...)
		}
		if killed != nil {
			assert.Equal(t, lenBefore-1, len(nodes))

			// wait until it's actually dead/out
			WaitUntilUnresponsive(killed)
		}

		killedStr := "(none)"
		if killed != nil {
			killedStr = fmt.Sprintf("%v", killed.node)
		}

		idTo := RandomID()
		start, _, _ := RandNode(nodes)
		from, _, _ := RandNode(nodes)
		expected := expectedRoot(idTo, nodes)
		root, err, numhops, hops := from.findRoot(start.node, idTo)

		testPass := true
		testPass = testPass && assert.Nil(t, err)
		testPass = testPass && assert.Equal(t, expected.node, root,
			"Routing from %v to %v, starting at %v, killed: %v (Nodes: %v)",
			from.node, idTo, start.node, killedStr, ids)
		testPass = testPass && assert.True(t, numhops <= NETWORK_SIZE)
		if !testPass {
			Debug.Printf("expected: %v, actual: %v, killed: %v  (numhops: %v)",
				expected.node, root, killedStr, numhops)
			PrintRoute(start.node, idTo, hops, nodes)
		}
	}

	// root itself drops out
	nodes, _ = CreateTapestryNetworkRand(t, NETWORK_SIZE)
	if nodes == nil {
		// error already created
		return
	}

	for len(nodes) > 3 { // need 2 for routing
		lenBefore := len(nodes)
		idTo := RandomID()

		// remove root every time
		killed := expectedRoot(idTo, nodes)
		for i, node := range nodes {
			if node.node.Equals(killed.node) {
				if graceful {
					killed.Leave()
				} else {
					killed.Kill()
				}
				nodes = append(nodes[:i], nodes[i+1:]...)
				break
			}
		}
		assert.Equal(t, lenBefore-1, len(nodes))

		// wait until it's actually dead/out
		WaitUntilUnresponsive(killed)

		start, _, _ := RandNode(nodes)
		from, _, _ := RandNode(nodes)
		expectedAfterKill := expectedRoot(idTo, nodes)
		root, err, numhops, hops := from.findRoot(start.node, idTo)

		testPass := true
		testPass = testPass && assert.Nil(t, err)
		testPass = testPass && assert.Equal(t, expectedAfterKill.node, root,
			"Routing from %v to %v, starting at %v, killed: %v (Nodes: %v)",
			from.node, idTo, start.node, killed.node, nodes)
		testPass = testPass && assert.True(t, numhops <= NETWORK_SIZE)
		if !testPass {
			Debug.Printf("expected: %v, actual: %v, killed: %v (numhops: %v)",
				expectedAfterKill.node, root, killed.node, numhops)
			PrintRoute(start.node, idTo, hops, nodes)
		}
	}

	// all nodes in route dropping out (ONLY error case)
	nodes, _ = CreateTapestryNetworkRand(t, NETWORK_SIZE)
	if nodes == nil {
		// error already created
		return
	}

	// kill all nodes but one after populating, then try and route to one that was killed
	idTo := nodes[0].node.Id // make sure to remove this
	for i := 0; i < NETWORK_SIZE-1; i++ {
		if graceful {
			nodes[i].Leave()
		} else {
			nodes[i].Kill()
		}
		// wait until it's actually dead/out
		WaitUntilUnresponsive(nodes[i])
	}

	onlyNode := nodes[NETWORK_SIZE-1]
	_, err, numhops, _ := onlyNode.findRoot(onlyNode.node, idTo)
	assert.Equal(t, nil, err)
	assert.True(t, numhops <= NETWORK_SIZE)
}

// concrete tests of failures; oracle does the rest
// (to the extent that it can; it's harder to tell in the oracle
// if an object should actually be stored or has been completely lost)
func TestFindRootFailuresKill(t *testing.T) {
	findRootFailures(t, false)
}

func TestFindRootFailuresLeave(t *testing.T) {
	findRootFailures(t, true)
}
