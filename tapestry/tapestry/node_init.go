/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines global constants and functions to create and join a new
 *  node into a Tapestry mesh, and functions for altering the routing table
 *  and backpointers of the local node that are invoked over RPC.
 */

package tapestry

import (
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	// Uncomment for xtrace
	// util "github.com/brown-csci1380/tracing-framework-go/xtrace/grpcutil"
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)

const BASE = 16       // The base of a digit of an ID.  By default, a digit is base-16.
const DIGITS = 40     // The number of digits in an ID.  By default, an ID has 40 digits.
const RETRIES = 3     // The number of retries on failure. By default we have 3 retries.
const K = 10          // During neighbor traversal, trim the neighborset to this size before fetching backpointers. By default this has a value of 10.
const SLOTSIZE = 3    // The each slot in the roting table should store this many nodes. By default this is 3.
const MAX_HOPS = 1000 // Max number of hops while routing

const REPUBLISH = 1 * time.Second //10 * time.Second // Object republish interval for nodes advertising objects.
const TIMEOUT = 3 * time.Second   //25 * time.Second   // Object timeout interval for nodes storing objects.

const PRINT_ERRS = true

// The main struct for the local Tapestry node. Methods can be invoked locally on this struct.
type Node struct {
	node           RemoteNode    // The ID and address of this node
	table          *RoutingTable // The routing table
	backpointers   *Backpointers // Backpointers to keep track of other nodes that point to us
	locationsByKey *LocationMap  // Stores keys for which this node is the root
	blobstore      *BlobStore    // Stores blobs on the local node
	server         *grpc.Server
}

func (local *Node) String() string {
	return fmt.Sprintf("Tapestry Node %v at %v", local.node.Id, local.node.Address)
}

func (n *Node) GetRemoteSelf() *RemoteNode {
	return &n.node
}

// Called in tapestry initialization to create a tapestry node struct
func NewTapestryNode(node RemoteNode) *Node {
	serverOptions := []grpc.ServerOption{}
	// Uncomment for xtrace
	// serverOptions = append(serverOptions, grpc.UnaryInterceptor(util.XTraceServerInterceptor))
	n := new(Node)

	n.node = node
	n.table = NewRoutingTable(node)
	n.backpointers = NewBackpointers(node)
	n.locationsByKey = NewLocationMap()
	n.blobstore = NewBlobStore()
	n.server = grpc.NewServer(serverOptions...)

	return n
}

// Start a tapestry node on the specified port. Optionally, specify the address
// of an existing node in the tapestry mesh to connect to; otherwise set to "".
func Start(port int, connectTo string) (*Node, error) {
	return start(RandomID(), port, connectTo)
}

// Private method, useful for testing: start a node with the specified ID rather than a random ID.
func start(id ID, port int, connectTo string) (tapestry *Node, err error) {

	// Create the RPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	// Get the hostname of this machine
	name, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Unable to get hostname of local machine to start Tapestry node. Reason: %v", err)
	}

	// Get the port we are bound to
	_, actualport, err := net.SplitHostPort(lis.Addr().String()) //fmt.Sprintf("%v:%v", name, port)
	if err != nil {
		return nil, err
	}

	// The actual address of this node
	address := fmt.Sprintf("%s:%s", name, actualport)

	// Uncomment for xtrace
	// xtr.NewTask("startup")
	// Trace.Print("Tapestry Starting up...")
	// xtr.SetProcessName(fmt.Sprintf("Tapestry %X... (%v)", id[:5], address))

	// Create the local node
	tapestry = NewTapestryNode(RemoteNode{Id: id, Address: address})
	fmt.Printf("Created tapestry node %v\n", tapestry)

	RegisterTapestryRPCServer(tapestry.server, tapestry)
	//fmt.Printf("Registered RPC Server\n")
	go tapestry.server.Serve(lis)

	// If specified, connect to the provided address
	if connectTo != "" {
		// Get the node we're joining
		node, err := SayHelloRPC(connectTo, tapestry.node)
		if err != nil {
			return nil, fmt.Errorf("Error joining existing tapestry node %v, reason: %v", address, err)
		}
		err = tapestry.Join(node)
		if err != nil {
			return nil, err
		}
	}

	return tapestry, nil
}

// Invoked when starting the local node, if we are connecting to an existing Tapestry.
//
// - Find the root for our node's ID
// - Call AddNode on our root to initiate the multicast and receive our initial neighbor set
// - Iteratively get backpointers from the neighbor set and populate routing table
func (local *Node) Join(otherNode RemoteNode) (err error) {
	Debug.Println("Joining", otherNode)

	// only set error at end if our retries run out
	var possibleErr error = nil

	// student: added retries
	for try := 0; try < RETRIES; try++ {
		// Route to our root
		root, possibleErr, _, _ := local.findRoot(otherNode, local.node.Id)
		if possibleErr != nil {
			possibleErr = fmt.Errorf("Error joining existing tapestry node %v, reason: %v", otherNode, err)
			continue // try again
		} else {
			Debug.Printf("New node %v routed to %v as root", local.node, root)
		}

		// fatal error if node already exists
		if root.Id.String() == local.node.Id.String() {
			err = fmt.Errorf("node with id %v already exists in network!", root.Id)
			return
		}

		// Add ourselves to our root by invoking AddNode on the remote node
		neighbors, possibleErr := root.AddNodeRPC(local.node)
		if possibleErr != nil {
			possibleErr = fmt.Errorf("Error adding ourselves to root node %v, reason: %v", root, err)
			continue // try again
		}

		// Add the neighbors to our local routing table.
		for _, n := range neighbors {
			local.addRoute(n)
		}

		// Traverse backpointers to add nodes BEYOND just those that have
		// the prefix shared between us and the root.
		// We go through all levels of the routing table, requesting backpointers
		// from our current set of known nodes (this starts out as the initial neighbors)
		// see https: //piazza.com/class/jc9twx8s4aq2je?cid=290
		possibleErr = local.traverseBackpointers(neighbors, 0)
		if possibleErr != nil {
			possibleErr = fmt.Errorf("Error traversing backpointers for node %v, reason: %v", local, err)
			continue // try again
		}
		// if we make it through without errors, finish
		break
	}

	if possibleErr == nil {
		Debug.Printf("%v successfully joined network!", local)
	}
	return possibleErr
}

// SortListByCloseness implements insertionSort for sorting the list by closeness
func (local *Node) SortListByCloseness(nodes []RemoteNode) []RemoteNode {
	// makes a new slice with correct capacity
	result := make([]RemoteNode, 0)

	for _, node := range nodes {
		if len(result) == 0 {
			// if nothing, add the first element
			result = append(result, node)
			continue
		}
		i := local.FindInsertionPosition(node, result)
		// create new list with one greater capacity
		appended := make([]RemoteNode, len(result)+1)
		// Use copy to copy over end into new appended slice
		copy(appended[i+1:], result[i:])
		// Store the new value.
		appended[i] = node
		// copy over the beginning i elements
		copy(appended[:i], result[:i])
		result = appended
	}
	return result
}

func (local *Node) FindInsertionPosition(node RemoteNode, nodes []RemoteNode) int {
	for i := range nodes {
		// if the node we're trying to insert is closer than the one at the ith position
		if local.node.Id.Closer(node.Id, nodes[i].Id) {
			return i
		}
	}
	return len(nodes)
}

// Appends the given neighbors ignoring duplicates and returns the result
func InsertWithoutDuplicates(neighbors []RemoteNode, newNeighbors []RemoteNode) (results []RemoteNode) {
	results = neighbors
	for _, neighbor := range newNeighbors {
		if !neighbor.ContainedIn(neighbors) {
			results = append(results, neighbor)
		}
	}
	return
}

// Traverses the backpointers of the given set of neighbors, starting at the given
// level and descending down all higher levels.
//
// - At each level, also limits the neighbor set for the next iteration to K
func (local *Node) traverseBackpointers(neighbors []RemoteNode, level int) (err error) {
	if level >= 0 {
		// copy neighbors set as basis for nextNeighbors
		nextNeighbors := make([]RemoteNode, 0)

		for _, neighbor := range neighbors {
			// for each neighbor, grab all backpointers it has
			// that exist at this level in that neighbor's routing table
			// (also pass our node so that that node can add us to it's routing table)
			backpointers, err := neighbor.GetBackpointersRPC(local.node, level)
			if err != nil {
				// continue to try and add more backpointers if we get an err,
				// but return error if nothing else occurs
				Error.Printf("Unreachable node notice: %v unreachable while traversing backpointers for %v", neighbor, local)
				continue
			}

			// append ALL the backpointers, as a set
			nextNeighbors = InsertWithoutDuplicates(nextNeighbors, backpointers)
		}

		// add all to our routing table
		for _, neighbor := range nextNeighbors {
			//Debug.Printf("Added %v to %v via backpointer traversal", neighbor, local.node)
			local.addRoute(neighbor)
		}

		// trim the list of neighbors to K (they're already sorted such
		// that the best (closest) K are at the front, and duplicates have
		// been removed (i.e. ignored when adding)
		nextNeighbors = local.SortListByCloseness(nextNeighbors)
		if len(nextNeighbors) > K {
			nextNeighbors = nextNeighbors[:K]
		}

		// move on to the next level, using our updated set of neighbors as a starting point
		err = local.traverseBackpointers(nextNeighbors, level-1)
	}

	return
}

// We are the root node for some new node joining the tapestry.
//
// - Begin the acknowledged multicast
// - Return the neighborset from the multicast
func (local *Node) AddNode(node RemoteNode) (neighborset []RemoteNode, err error) {
	if local.node.Equals(node) {
		err := fmt.Errorf("Node with id %v already exists in the network!", local.node.Id)
		Error.Print(err)
		return nil, err
	}
	sharedLen := SharedPrefixLength(node.Id, local.node.Id)
	return local.AddNodeMulticast(node, sharedLen)
}

// Transfers any objects to newnode that are stored on this node
// but should be stored on newnode
func (local *Node) TransferRelevantObjects(newNode RemoteNode) {
	transfers := local.locationsByKey.GetTransferRegistrations(local.node, newNode)
	if len(transfers) > 0 {
		Debug.Printf("Transferring %d objects from here to %v", len(transfers), newNode)
		err := newNode.TransferRPC(local.node, transfers)
		if err != nil {
			Debug.Printf("Unreachable node notice: transfer of objects to %v failed: %v", newNode, err)
			// if transfer failed, add back the transfers into our store so they're not lost
			local.locationsByKey.RegisterAll(transfers, TIMEOUT)
		}
	}
}

// A new node is joining the tapestry, and we are a need-to-know node participating in the multicast.
//
// - Propagate the multicast to the specified row in our routing table
// - Await multicast response and return the neighborset
// - Add the route for the new node
// - Begin transfer of appropriate replica info to the new node
func (local *Node) AddNodeMulticast(newnode RemoteNode, level int) (neighbors []RemoteNode, err error) {
	//Debug.Printf("%v: add node multicast %v at level %v\n", local.node, newnode, level)

	// only try to continue searching for backpointers if we're not
	// too far down the table (we still want to add the newnode to
	// our routing table and transfer objects)
	results := make([]RemoteNode, 0)
	if level < DIGITS {
		// start out multicast at all nodes at the given level in our table
		// (the nodes that share at least that prefix length with the newnode)
		// (append the local node to the results, because we need it to find more
		// results and it's explicitly ignored in GetLevel)
		targets := append(local.table.GetLevel(level), local.node)

		// then, we'll compile all the nodes at the next level (next higher shared
		// prefix length, and add them to our list of candidate object transfer nodes)
		for _, target := range targets {
			targetResults, err := target.AddNodeMulticastRPC(newnode, level+1)
			if err != nil {
				// continue to try and add more targetResults i, node.node.Id.String() if we get an err,
				// but return error if nothing else occurs
				Debug.Printf("Unreachable node notice: target %v unreachable while performing multicast for %v from %v: %v", target, newnode, local, err)
			}
			results = InsertWithoutDuplicates(results, targetResults)
		}

		// merge our original targets and all their results of the multicast,
		// removing any duplicates
		results = InsertWithoutDuplicates(results, targets)
	}

	// make sure we add the newnode to our routing table if possible
	// (err will print message but there's nothing to do here)
	local.addRoute(newnode)

	// in the background, start transferring any objects from here that should be on the node
	go local.TransferRelevantObjects(newnode)

	// return and results/errors we had
	return results, err
}

// Add the from node to our backpointers, and possibly add the node to our
// routing table, if appropriate
func (local *Node) AddBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Add(from) {
		//Debug.Printf("%v: added backpointer %v\n", local.node, from)
	}
	// no error because add only doesn't add current node
	local.addRoute(from)
	return
}

// Remove the from node from our backpointers
func (local *Node) RemoveBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Remove(from) {
		//Debug.Printf("%v: removed backpointer %v\n", local.node, from)
	}
	// no error because remove only doesn't remove current node
	return
}

// Get all backpointers at the level specified, and possibly add the node to our
// routing table, if appropriate
func (local *Node) GetBackpointers(from RemoteNode, level int) (backpointers []RemoteNode, err error) {
	Debug.Printf("Sending level %v backpointers to %v\n", level, from)
	backpointers = local.backpointers.Get(level)
	local.addRoute(from)
	return
}

// The provided nodes are bad and we should discard them
// - Remove each node from our routing table
// - Remove each node from our set of backpointers
func (local *Node) RemoveBadNodes(badnodes []RemoteNode) (err error) {
	for _, badnode := range badnodes {
		if local.table.Remove(badnode) {
			Debug.Printf("Removed bad node %v\n", badnode)
		}
		if local.backpointers.Remove(badnode) {
			Debug.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}
	return
}

// Utility function that adds a node to our routing table.
//
// - Adds the provided node to the routing table, if appropriate.
// - If the node was added to the routing table, notify the node of a backpointer
// - If an old node was removed from the routing table, notify the old node of a removed backpointer
func (local *Node) addRoute(node RemoteNode) (err error) {
	// try to add node to our table
	added, removed := local.table.Add(node)

	// if the node was added, add us as a backpointer for it
	if added {
		err = node.AddBackpointerRPC(local.node)
		if err != nil {
			Debug.Printf("Unreachable node notice: couldn't add backpointer for new node route %v: ", node)
		}
	}

	// if a node was removed when we added the other one,
	// remove the backpointer it has to us (we no longer reference it)
	if removed != nil {
		err = removed.RemoveBackpointerRPC(local.node)
		if err != nil {
			Debug.Printf("Unreachable node notice: couldn't remove backpointer for evicted node %v: ")
		}
	}

	return
}
