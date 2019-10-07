/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines global constants and functions to create and join a new
 *  node into a Tapestry mesh, and functions for altering the routing table
 *  and backpointers of the local node that are invoked over RPC.
 */

package tapestry

//__BEGIN_TA__
import (
	"fmt"
	"net"
	"os"
	"time"

	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
	// util "github.com/brown-csci1380/tracing-framework-go/xtrace/grpcutil"
	"google.golang.org/grpc"
)

//__END_TA__

/*__BEGIN_STUDENT__
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
__END_STUDENT__*/
const BASE = 16    // The base of a digit of an ID.  By default, a digit is base-16.
const DIGITS = 40  // The number of digits in an ID.  By default, an ID has 40 digits.
const RETRIES = 3  // The number of retries on failure. By default we have 3 retries.
const K = 10       // During neighbor traversal, trim the neighborset to this size before fetching backpointers. By default this has a value of 10.
const SLOTSIZE = 3 // The each slot in the routing table should store this many nodes. By default this is 3.

const REPUBLISH = 10 * time.Second // Object republish interval for nodes advertising objects.
const TIMEOUT = 25 * time.Second   // Object timeout interval for nodes storing objects.

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

// Called in tapestry initialization to create a tapestry node struct
func newTapestryNode(node RemoteNode) *Node {
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
	tapestry = newTapestryNode(RemoteNode{Id: id, Address: address})
	//fmt.Printf("Created tapestry node %v\n", tapestry)
	Trace.Printf("Created tapestry node")

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
		fmt.Printf("Successfully connected to %v\n", connectTo)
	}

	return tapestry, nil
}

// Invoked when starting the local node, if we are connecting to an existing Tapestry.
//
// - Find the root for our node's ID
// - Call AddNode on our root to initiate the multicast and receive our initial neighbor set
// - Iteratively get backpointers from the neighbor set and populate routing table
func (local *Node) Join(otherNode RemoteNode) (err error) {
	//__BEGIN_TA__
	// xtr.AddTags("join")
	//__END_TA__
	Debug.Println("Joining", otherNode)

	// Route to our root
	root, _, err := local.findRoot(otherNode, local.node.Id)
	if err != nil {
		return fmt.Errorf("Error joining existing tapestry node %v, reason: %v", otherNode, err)
	}

	// Add ourselves to our root by invoking AddNode on the remote node
	neighbors, err := root.AddNodeRPC(local.node)
	if err != nil {
		return fmt.Errorf("Error adding ourselves to root node %v, reason: %v", root, err)
	}

	// Add the neighbors to our local routing table.
	for _, n := range neighbors {
		local.addRoute(n)
	}

	// __BEGIN_TA__

	Trace.Println("getting backpointers")

	// Start traversing backpointers to populate the routing table
	p := SharedPrefixLength(root.Id, local.node.Id)
	for ; p >= 0; p-- {
		Trace.Println("backpointers level", p)
		// Trim the list to the closest K nodes
		// TODO: sort the list; for now not sorted
		if len(neighbors) > K {
			neighbors = neighbors[:K]
		}

		// Get the backpointers for these neighbors
		backpointers := []RemoteNode{}
		results := make(chan []RemoteNode)
		getBackpointers := func(from RemoteNode) {
			Trace.Println("Getting backpointers from", from)
			pointers, err := from.GetBackpointersRPC(local.node, p)
			if err != nil {
				// xtr.AddTags("bad node")
				local.RemoveBadNodes([]RemoteNode{from})
			}
			results <- pointers
		}

		// Kick off the goroutines to get backpointers
		for _, node := range neighbors {
			go getBackpointers(node)
		}

		// Merge the results
		for i := 0; i < len(neighbors); i++ {
			for _, backpointer := range <-results {
				if backpointer != local.node {
					backpointers = append(backpointers, backpointer)
				}
			}
		}

		// Add the backpointers to the routing table
		for _, b := range backpointers {
			local.addRoute(b)
		}

		// Update the neighbors
		neighbors = append(neighbors, backpointers...)
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement the backpointer traversal portion of Join
	// __END_STUDENT__

	return nil
}

// We are the root node for some new node joining the tapestry.
//
// - Begin the acknowledged multicast
// - Return the neighborset from the multicast
func (local *Node) AddNode(node RemoteNode) (neighborset []RemoteNode, err error) {
	return local.AddNodeMulticast(node, SharedPrefixLength(node.Id, local.node.Id))
}

// A new node is joining the tapestry, and we are a need-to-know node participating in the multicast.
//
// - Propagate the multicast to the specified row in our routing table
// - Await multicast response and return the neighborset
// - Add the route for the new node
// - Begin transfer of appropriate replica info to the new node
func (local *Node) AddNodeMulticast(newnode RemoteNode, level int) (neighbors []RemoteNode, err error) {
	Debug.Printf("Add node multicast %v at level %v\n", newnode, level)
	// __BEGIN_TA__

	// Get multicast targets
	nodes := local.table.GetLevel(level)
	done := make(chan []RemoteNode)

	notify := func(destination RemoteNode) {
		Trace.Println("Notifying", destination)
		neighbors, err := destination.AddNodeMulticastRPC(newnode, level+1)
		if err != nil {
			local.RemoveBadNodes([]RemoteNode{destination})
		}
		done <- neighbors
	}

	// Kick off asynchronous multicast
	for _, node := range nodes {
		go notify(node)
	}

	// If we're at level DIGITS, transfer keys, otherwise multicast to 1 level down
	if level == DIGITS {
		// Transfer keys to the new node
		go func() {
			// xtr.NewTask("transferkeys")
			Trace.Print("Beginning transfer keys")
			// Add the new node to the routing table
			local.addRoute(newnode)

			// Get the data to transfer
			objects := local.locationsByKey.GetTransferRegistrations(local.node, newnode)

			// Transfer the data
			err := newnode.TransferRPC(local.node, objects)

			if err != nil {
				// On error, remove the new node from our table, and reinsert the transferred data
				local.RemoveBadNodes([]RemoteNode{newnode})
				local.locationsByKey.RegisterAll(objects, TIMEOUT)
			}
		}()

		neighbors = append(neighbors, local.node)
	} else {
		// Multicast the local node at the next level down
		neighbors, err = local.AddNodeMulticast(newnode, level+1)

		// Append returned neighbors
		for i := 0; i < len(nodes); i++ {
			neighbors = append(neighbors, (<-done)...)
		}
	}

	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// Add the from node to our backpointers, and possibly add the node to our
// routing table, if appropriate
func (local *Node) AddBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Add(from) {
		Debug.Printf("Added backpointer %v\n", from)
	}
	local.addRoute(from)
	return
}

// Remove the from node from our backpointers
func (local *Node) RemoveBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed backpointer %v\n", from)
	}
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
	// __BEGIN_TA__
	added, previous := local.table.Add(node)
	go func() {
		if previous != nil {
			// Notify of backpointer removal. Error doesn't matter because previous is no longer in table
			previous.RemoveBackpointerRPC(local.node)
		}

		if added {
			Debug.Printf("Added %v to routing table\n", node)
			// Try notifying of a backpointer
			err := node.AddBackpointerRPC(local.node)

			if err != nil {
				Debug.Printf("Backpointer notification to %v failed\n", node)
				// If backpointer notification fails, remove the node from the routing table
				local.table.Remove(node)

				// Attempt reinsertion of previous
				if previous != nil {
					local.addRoute(*previous)
				}
			}
		}
	}()
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}


func (n *Node) GetRemoteSelf() *RemoteNode {
	return &n.node
}
