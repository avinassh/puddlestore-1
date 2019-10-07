package tapestry

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

// cleans up debug prints in oracle
const DEBUG_ON_TESTING = true
const DATASIZE = 512
const MAX_CMD_DELAY_MS = 10
const PUBLISH_POLL_DUR = 500 * time.Millisecond
const TIMINGS_BUFFER = 1 * time.Second

const PUB = 0
const GET = 1
const ADD = 2
const LEAVE = 3
const DEL = 4
const KILL = 5

const NUM_COMMANDS = 6

type Tapestry struct {
	Nodes []*Node           //All nodes currently in the network
	Keys  []string          //All keys currently stored in network
	Blobs map[string][]byte //Map from keys to the data they store
	Rand  *rand.Rand        //seeded random number generator
	sync.Mutex
}

func NewTapestry() *Tapestry {
	SetDebug(DEBUG_ON_TESTING)
	t := new(Tapestry)
	t.Nodes = make([]*Node, 0)
	t.Keys = make([]string, 0)
	t.Blobs = make(map[string][]byte)
	t.Rand = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return t
}

func RandNode(nodes []*Node) (node *Node, i int, err error) {
	err = nil
	if len(nodes) == 0 {
		err = errors.New("No nodes")
		return
	}
	i = rand.Intn(len(nodes))
	node = nodes[i]
	return
}

func (tp *Tapestry) RandNode() (node *Node, i int, err error) {
	return RandNode(tp.Nodes)
}

func (tp *Tapestry) RandKey() (key string, i int, err error) {
	err = nil
	if len(tp.Keys) == 0 {
		err = errors.New("No keys")
		return
	}
	i = tp.Rand.Intn(len(tp.Keys))
	key = tp.Keys[i]
	return
}

func (tp *Tapestry) NodeFromRemoteNode(rn RemoteNode) (node *Node, err error) {
	err = nil
	if len(tp.Nodes) == 0 {
		err = errors.New("No Nodes")
		return
	}
	node = tp.Nodes[0]
	for _, n := range tp.Nodes {
		if n.node.Id.String() == rn.Id.String() {
			node = n
			break
		}
	}
	return
}

// Removes all keys associated with a given Node
func (tp *Tapestry) RemoveKeysAtNode(t *testing.T, node *Node) {
	for key := range node.blobstore.blobs {

		// delete from Blobs
		delete(tp.Blobs, key)

		// delete from Keys
		tp.RemoveKey(t, key)
	}
}

// Remove key from tp.Keys
func (tp *Tapestry) RemoveKey(t *testing.T, key string) {
	for i, k := range tp.Keys {
		if k == key {
			tp.Keys = append(tp.Keys[:i], tp.Keys[i+1:]...)
			return
		}
	}
	t.Errorf("Key not found in Tapestry")
}

// Returns a sequence of n commands
func GenerateCommands(n int) []int {
	rand.Seed(time.Now().UTC().UnixNano())
	c := make([]int, n+1)
	for i := 0; i < n; i++ {
		c[i] = int(rand.Int31n(NUM_COMMANDS))
	}
	return c
}

// Publishes a random object from a random node
func Publish(t *testing.T, tp *Tapestry) {
	tp.Lock()
	node, _, err := tp.RandNode()
	if err == nil {
		key := RandString(8)
		data := make([]byte, DATASIZE)
		tp.Rand.Read(data)           //fills data with random bytes
		err := node.Store(key, data) //publish data
		if err != nil {
			t.Errorf("Error %v while publishing %v to node %v", err, key, node.node.Id.String())
		} else {
			WaitUntilPublished(t, key, node, tp.Nodes)
			published := IsPublished(t, key, node, tp.Nodes)
			if published {
				Out.Printf("Key %v published to node %v", key, node.node.Id.String())
				//if succesful, update tp
				tp.Keys = append(tp.Keys, key)
				tp.Blobs[key] = data
			} else {
				t.Errorf("Key %v was not actually published to node %v", key, node.node.Id)
			}
		}
	}
	tp.Unlock()
}

// Tries to obtain a random key from a random node
func Retrieve(t *testing.T, tp *Tapestry) {
	tp.Lock()
	key, _, err1 := tp.RandKey()
	node, _, err2 := tp.RandNode()

	//tests get functionality
	if err1 == nil && err2 == nil {
		blob, err := node.Get(key)
		if err != nil {
			ErrorPrintf(t, "%v during get %v (hash %v) from %v", err, key, Hash(key), node)
		} else if !bytes.Equal(blob, tp.Blobs[key]) {
			t.Errorf("Data corruption during get %v (hash %v) from %v", key, Hash(key), node)
		} else {
			Out.Printf("Get of key %v (hash %v) successful from %v", key, Hash(key), node)
		}
	}
	tp.Unlock()
}

// Adds a random node to the network
func Add(t *testing.T, tp *Tapestry) {
	tp.Lock()
	node, _, err := tp.RandNode()
	addr := ""
	if err == nil {
		// if RandNode found, connect it to newNode
		addr = node.node.Address
	}

	n, err := Start(0, addr)
	if err != nil {
		Out.Printf("%v while trying to connect new node to address %v", err, node.node.Address)
		t.Errorf("%v while trying to connect new node to address %v", err, node.node.Address)
	} else {
		if node != nil {
			Out.Printf("node %v connected to node %v", n.node.Id.String(), node.node.Id.String())
		} else {
			Out.Printf("node %v connected as new tapestry", n.node.Id.String())
		}

		// add Node to list
		tp.Nodes = append(tp.Nodes, n)
	}
	tp.Unlock()
}

// Clean exit of arbitrary node
func Leave(t *testing.T, tp *Tapestry) {
	tp.Lock()
	node, i, err := tp.RandNode()
	if err == nil {
		err = node.Leave()
		if err != nil {
			// leave failed?
			Debug.Printf("Unreachable node notice (test): clean exit of node %v failed", node.node.Id.String())
		} else {
			WaitUntilUnresponsive(node)
			Out.Printf("%v left succesfully", node)
			// remove keys associated with node
			tp.RemoveKeysAtNode(t, node)
			// remove from nodes
			tp.Nodes = append(tp.Nodes[:i], tp.Nodes[i+1:]...)
		}
	}
	tp.Unlock()
}

// Deletes an arbitrary key from an arbitrary node
// by picking an arbitrary node and checking its
// blobstore
func Delete(t *testing.T, tp *Tapestry) {
	tp.Lock()
	node, _, err := tp.RandNode()
	if err == nil {
		// obtain a random key
		var key string
		for key = range node.blobstore.blobs {
			break
		}

		// if there is some key stored here
		if key != "" {
			if node.Remove(key) {
				// successful deletion
				delete(tp.Blobs, key)
				tp.RemoveKey(t, key)
				Out.Printf("Successfully deleted %v from %v", key, node)
			} else {
				t.Errorf("Failed to delete key %v from %v (wasn't stored)", key, node)
			}
		}
	}
	tp.Unlock()
}

// Simulates non-clean exit of node
func Kill(t *testing.T, tp *Tapestry) {
	tp.Lock()
	node, i, err := tp.RandNode()
	if err == nil {
		node.Kill()
		WaitUntilUnresponsive(node)
		Out.Printf("Node %v at %v killed", node.node.Id.String(), node.node.Address)
		// remove keys associated
		tp.RemoveKeysAtNode(t, node)
		tp.Nodes = append(tp.Nodes[:i], tp.Nodes[i+1:]...)
	}
	tp.Unlock()
}

// Runs n commands on a tapestry network that may be initialized however
func RunCommands(t *testing.T, tp *Tapestry, c []int) {
	for _, comm := range c {
		switch comm {
		case PUB:
			Publish(t, tp)
			break
		case GET:
			Retrieve(t, tp)
			break
		case ADD:
			Add(t, tp)
			break
		case LEAVE:
			Leave(t, tp)
			break
		case DEL:
			Delete(t, tp)
		case KILL:
			Kill(t, tp)
		default:
			t.Errorf("invalid command")
		}
		time.Sleep(time.Duration(tp.Rand.Intn(MAX_CMD_DELAY_MS)) * time.Millisecond)
	}
}
