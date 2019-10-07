package tapestry

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Wrapper for t.ErrorF that prints the message immediately
func ErrorPrintf(t *testing.T, fmt string, args ...interface{}) {
	printCaller()
	t.Errorf(fmt, args...)
	Error.Printf(fmt, args...)
}

func ErrorIfError(t *testing.T, err error, prefix string, args ...interface{}) {
	if err != nil {
		ErrorPrintf(t, "%s%v", fmt.Sprintf(prefix, args), err)
	}
}

// Creates a RemoteNode with the given ID, nil if error
func RemoteNodeFromID(id string) *RemoteNode {
	rn := new(RemoteNode)
	i, err := ParseID(id)
	rn.Id = i
	if err != nil {
		return nil
	}
	return rn
}

// Returns the node in nodes matching the given RemoteNode
func NodeFromRemoteNode(remNode RemoteNode, nodes []*Node) (node *Node, err error) {
	for _, node = range nodes {
		if node.node.Equals(remNode) {
			return node, nil
		}
	}
	return nil, fmt.Errorf("Expected node %v not found in nodes %v", remNode, nodes)
}

// Creates a tapestry network with nodes of the given ids
func CreateTapestryNetwork(t *testing.T, ids []ID) (nodes []*Node) {
	SetDebug(true)

	Out.Printf("Creating tapestry network of size %d", len(ids))
	nodes = make([]*Node, len(ids))
	prevNode, err := start(ids[0], 0, "")
	if err != nil {
		ErrorPrintf(t, "Couldn't start initial node: %s", err)
		return nil

	} else {
		nodes[0] = prevNode
		for i := 1; i < len(ids); i++ {
			// connect to previous node, specify 0 for rand port
			nodes[i], err = start(ids[i], 0, prevNode.node.Address)
			if err != nil {
				ErrorPrintf(t, "Couldn't start additional node: %s", err)
				return nil
			}
			prevNode = nodes[i]
		}
		return
	}
}

// Creates a tapestry network of given size with random IDs
func CreateTapestryNetworkRand(t *testing.T, size int) (nodes []*Node, ids []ID) {
	ids = make([]ID, size)
	for i := 0; i < size; i++ {
		ids[i] = RandomID()
	}
	nodes = CreateTapestryNetwork(t, ids)
	return
}

// Returns whether the given key is published by the given node,
// (is at it's root)
func IsPublished(t *testing.T, key string, node *Node, nodes []*Node) bool {
	hash := Hash(key)
	keyRoot, err, _, _ := node.findRoot(node.node, hash)
	ErrorIfError(t, err, "IsPublished finding root: ")

	keyRootNode, err := NodeFromRemoteNode(keyRoot, nodes)
	if err != nil {
		t.Errorf("keyRoot was not in nodes list!")
		return false
	}
	rootNodesForKey := keyRootNode.locationsByKey.data[key]

	for possibleNode := range rootNodesForKey {
		if possibleNode == node.node {
			return true
		}
	}
	return false
}

// Waits until the given key is published from the given node, given the set of nodes
func WaitUntilPublished(t *testing.T, key string, node *Node, nodes []*Node) {
	start := time.Now()
	Out.Print("Waiting for at least one publish...")
	for !IsPublished(t, key, node, nodes) {
		time.Sleep(PUBLISH_POLL_DUR)
	}
	assert.InDelta(t, REPUBLISH, time.Since(start), float64(TIMINGS_BUFFER))
}

// Busy loops until the node reports dead
func WaitUntilUnresponsive(node *Node) {
	var err error = nil
	for err == nil {
		_, _, err = node.node.GetNextHopRPC(node.node.Id)
		time.Sleep(1 * time.Millisecond)
	}
}

// Returns some random string of length n
func RandString(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// Debug method: prints route and routing tables
// from start to id given nodes in route
func PrintRoute(start RemoteNode, dest ID, hops []RemoteNode, nodes []*Node) {
	Debug.Printf("=============Route=============")
	Debug.Printf("dest: %v, start: %v", dest, start.Id)
	for i, remNode := range hops {
		Debug.Printf("Hop %d: %v; table:", i, remNode.Id)
		node, err := NodeFromRemoteNode(remNode, nodes)
		if err == nil {
			node.PrintRoutingTable()
		} else {
			Error.Printf("Remote node not in given nodes!")
		}
	}
	Debug.Printf("==========End Route============")
}
