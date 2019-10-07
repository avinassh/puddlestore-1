/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

// __BEGIN_TA__
import (
	"fmt"
	"time"
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)

const NUM_SALTS = 3
const REPLICATION_NODES = 4

// __END_TA__
/* __BEGIN_STUDENT__
import (
	"fmt"
	// Uncomment for xtrace
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)
__END_STUDENT__ */

// Store a blob on the local node and publish the key to the tapestry.
// Will throw an error if publishing fails with *all* salts.
// With salts, if the root crashes, the item is still accessible.
func (local *Node) Store(key string, value []byte) (err error) {

	// Will contact up to REPLICATION_NODES other nodes from the routing table
	// and store at them too. Can be run in parallel.
	go func() {
		replicas := local.table.GetReplicas()
		for _, node := range replicas {
			err := node.TapestryReplicateRPC(key, value)
			if err != nil {
				Out.Printf("Successfully replicated %v at %v\n", key, node)
			}
		}
	}()

	return local.Replicate(key, value)
}

// Store a blob on the local node and publish the key to the tapestry.
// Will throw an error if publishing fails with *all* salts.
// With salts, if the root crashes, the item is still accessible.
func (local *Node) Replicate(key string, value []byte) (err error){
	salts := salts(key) //various salts of key
	successes := make([]chan bool, 0) //done channels of successful replicas
	var cause error //potential cause of error

	// TODO: make these goroutines for added performance
	for _, salt := range salts {
		done, err := local.Publish(salt)
		if err == nil {
			successes = append(successes, done)
		} else {
			cause = err
		}
	}

	if len(successes) > 0 {
		local.blobstore.Put(key, value, successes)
		//Out.Printf("Sucessfully stored %v at %v nodes", key, len(successes))
		return nil
	} else {
		Out.Printf("Failed to store any replicas for %v", key)
		return fmt.Errorf("all salts failed: %v", cause.Error())
	}
}

// Lookup a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *Node) Get(key string) ([]byte, error) {
	salts := salts(key)
	saltErr := make([]string, 0)
	errs := make([]error, 0)

	for _, salt := range salts {
		bytes, err := local.get(key, salt)
		if err != nil {
			saltErr = append(saltErr, salt)
			errs = append(errs, err)
		} else {
			return bytes, nil
		}
	}

	return nil, fmt.Errorf("failed for %v with errors %v", saltErr, errs)
}

/*
	Lookup a single salt of the key. Will use the salt to traverse to the root
	and then lookup the key at the root.
 */
func (local *Node) get(key string, salt string) ([]byte, error) {
	// Lookup the key
	Out.Printf("Looking for %v with salt %v", key, salt)
	replicas, err := local.Lookup(salt)
	if err != nil {
		return nil, err
	}
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas returned for key %v", key)
	}

	// Contact replicas
	var errs []error
	for _, replica := range replicas {
		blob, err := replica.BlobStoreFetchRPC(key)
		if err != nil {
			errs = append(errs, err)
		}
		if blob != nil {
			Out.Printf("Found %v", key)
			return *blob, nil
		}
	}

	return nil, fmt.Errorf("error contacting replicas, %v: %v", replicas, errs)
}


// Remove the blob from the local blob store and stop advertising
func (local *Node) Remove(key string) bool {
	return local.blobstore.Delete(key)
}

// Publishes the key in tapestry.
//
// - Route to the root node for the key
// - Register our ln node on the root
// - Start periodically republishing the key
// - Return a channel for cancelling the publish
// - Obtains the path from findRoot and loops over it, registering itself at each node
func (local *Node) Publish(key string) (done chan bool, err error) {
	// __BEGIN_TA__
	// xtr.NewTask("publish")
	publish := func(key string) error {
		Debug.Printf("Publishing %v\n", key)

		failures := 0
		for failures < RETRIES {

			// Route to the root node and obtain path
			root, path, err := local.findRoot(local.node, Hash(key))
			if err != nil {
				failures++
				continue
			}

			// Register this node on all nodes along the path, don't really care about catching errors though
			for _, rn := range path {
				rn.RegisterRPC(key, local.node)
			}

			// Register our local node on the root
			isRoot, err := root.RegisterRPC(key, local.node)
			if err != nil {
				// xtr.AddTags("failure")
				// Trace.Printf("failed to publish, bad node: %v\n", root)
				local.RemoveBadNodes([]RemoteNode{root})
				failures++
			} else if !isRoot {
				Trace.Printf("failed to publish to %v, not the root node", root)
				failures++
			} else {
				//Out.Printf("Successfully published %v on %v", key, root)
				return nil
			}
		}

		// xtr.AddTags("failure")
		// Trace.Printf("failed to publish %v (%v) due to %v/%v failures", key, Hash(key), failures, RETRIES)
		return fmt.Errorf("Unable to publish %v (%v) due to %v/%v failures", key, Hash(key), failures, RETRIES)
	}

	// Publish the key immediately
	err = publish(key)
	if err != nil {
		return
	}

	// Create the done channel
	done = make(chan bool)

	// Periodically republish the key
	go func() {
		ticker := time.NewTicker(REPUBLISH)

		for {
			select {
			case <-ticker.C:
				{
					// xtr.NewTask("republish")
					Trace.Printf("republishing %v", key)
					err := publish(key)
					if err != nil {
						Error.Print(err)
					}
				}
			case <-done:
				{
					Trace.Printf("Stopping advertisement of %v", key)
					//fmt.Printf("Stopping advertisement for %v\n", key)
					return
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

// Look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the replicas (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
// - Called on the salts for a given key
// - Because of path caching, if while routing to the root someone advertises
// as having the key (salt) in their location map, we will just take them and contact them
// - It does not matter that we do not route to the root every time to get *all* possible
// replicas as when Lookup is called with multiple salts,
func (local *Node) Lookup(key string) (nodes []RemoteNode, err error) {
	// __BEGIN_TA__
	// xtr.NewTask("lookup")
	Trace.Printf("Looking up %v", key)

	// Function to look up a key
	//lookup := func(key string) ([]RemoteNode, error) {
	//	// Look up the root node
	//	node, err := local.findRoot(local.node, Hash(key))
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	// Get the replicas from the root's location map
	//	isRoot, nodes, err := node.FetchRPC(key)
	//	if err != nil {
	//		return nil, err
	//	} else if !isRoot {
	//		return nil, fmt.Errorf("Root node did not believe it was the root node")
	//	} else {
	//		return nodes, nil
	//	}
	//}

	// Attempt up to RETRIES many times
	errs := make([]error, 0, RETRIES)
	for len(errs) < RETRIES {
		Debug.Printf("Looking up %v, attempt=%v\n", key, len(errs)+1)
		results, lookuperr := local.fetchAtRootOrSurrogate(local.node, key)
		if lookuperr != nil {
			Error.Println(lookuperr)
			errs = append(errs, lookuperr)
			continue
		}

		return results, nil
	}

	err = fmt.Errorf("%v failures looking up %v: %v", RETRIES, key, errs)

	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// Returns the best candidate from our routing table for routing to the provided ID
func (local *Node) GetNextHop(id ID) (morehops bool, nexthop RemoteNode, err error) {
	// __BEGIN_TA__
	nexthop = local.table.GetNextHop(id)
	morehops = (nexthop != local.node)
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// Register the specified node as an advertiser of the specified key.
// - Check that we are the root node for the key
// - Add the node to the location map
// - Kick off a timer to remove the node if it's not advertised again after a set amount of time
// - Can also Register a key even if you are not the root for path caching purposes
func (local *Node) Register(key string, replica RemoteNode) (isRoot bool, err error) {
	// __BEGIN_TA__
	if local.locationsByKey.Register(key, replica, TIMEOUT) {
		Debug.Printf("Register %v:%v (%v)\n", key, replica, Hash(key))
	}

	if local.table.GetNextHop(Hash(key)) == local.node {
		isRoot = true
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// - No longer required to be root node to Fetch
// - Return all nodes that are registered in the local location map for this key
// - Will return a nil slice if no nodes are registered
func (local *Node) Fetch(key string) (isRoot bool, replicas []RemoteNode, err error) {
	// __BEGIN_TA__
	if local.table.GetNextHop(Hash(key)) == local.node {
		isRoot = true
	}
	replicas = local.locationsByKey.Get(key)
	Debug.Printf("Lookup %v:%v (%v)\n", key, replicas, Hash(key))

	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// - Register all of the provided objects in the local location map
// - If appropriate, add the from node to our local routing table
func (local *Node) Transfer(from RemoteNode, replicamap map[string][]RemoteNode) (err error) {
	// __BEGIN_TA__
	if len(replicamap) > 0 {
		Debug.Printf("Registering objects from %v: %v\n", from, replicamap)
		local.locationsByKey.RegisterAll(replicamap, TIMEOUT)
	}
	local.addRoute(from)
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return nil
}

// Utility function for iteratively contacting nodes to get the root node for the provided ID.
//
// -  Starting from the specified node, iteratively contact nodes calling getNextHop until we reach the root node
// -  Also keep track of any bad nodes that errored during lookup
// -  At each step, notify the next-hop node of all of the bad nodes we have encountered along the way
// -  MODIFIED to return the entire path to the root node as a slice.
// -  Now returns (root, path(including start, excluding root), err)
func (local *Node) findRoot(start RemoteNode, id ID) (RemoteNode, []RemoteNode, error) {
	Debug.Printf("Routing to %v\n", id)
	//__BEGIN_TA__

	// Keep track of nodes traversed
	path := make([]RemoteNode, 0)

	// Keep track of faulty nodes along the way
	badnodes := NewNodeSet()

	// Use a stack to keep track of the traversed nodes
	nodes := make([]RemoteNode, 0)
	nodes = append(nodes, start)
	tail := 0

	// The remote node will indicate if it thinks its the root
	for len(nodes) > 0 {
		current := nodes[tail]
		nodes = nodes[:tail]
		tail--

		// If necessary, notify the next hop of accumulated bad nodes
		if badnodes.Size() > 0 {
			err := current.RemoveBadNodesRPC(badnodes.Nodes())
			if err != nil {
				// TODO: route error message somewhere?
				badnodes.Add(current)
				continue
			}
		}

		// Get the next hop
		morehops, next, err := current.GetNextHopRPC(id)
		if err != nil {
			// TODO: route error message somewhere?
			badnodes.Add(current)
			continue
		}

		// Check if we've arrived at the key's root
		if !morehops {
			return current, path, nil
		} else {
			// if not, add current node to path and continue
			path = append(path, current)
		}

		// Traverse the next node
		nodes = append(nodes, current, next)
		tail += 2
	}

	return RemoteNode{}, nil, fmt.Errorf("Unable to get root for %v, all nodes traversed were bad, starting from %v", id, start)
	// __END_TA__
	/* __BEGIN_STUDENT__
	    // TODO: students should implement this
	    return local.node, nil
	__END_STUDENT__ */
}

// Iteratively contacts nodes to get to the root for the given key.
// - If at any point we encounter a node that does advertise the given ID in a path cache, short circuit
// and return its list of replicas
// TODO: due to patch caching, certain nodes may never know of replicas to a given key
func (local *Node) fetchAtRootOrSurrogate(start RemoteNode, key string) ([]RemoteNode, error) {
	Debug.Printf("Routing to %v\n", key)
	//__BEGIN_TA__

	// for any key, id is the hash of the key
	id := Hash(key)

	// Keep track of faulty nodes along the way
	badnodes := NewNodeSet()

	// Use a stack to keep track of the traversed nodes
	nodes := make([]RemoteNode, 0)
	nodes = append(nodes, start)
	tail := 0

	// The remote node will indicate if it thinks its the root
	for len(nodes) > 0 {
		current := nodes[tail]
		nodes = nodes[:tail]
		tail--

		// If necessary, notify the next hop of accumulated bad nodes
		if badnodes.Size() > 0 {
			err := current.RemoveBadNodesRPC(badnodes.Nodes())
			if err != nil {
				// TODO: route error message somewhere?
				badnodes.Add(current)
				continue
			}
		}

		// Get the next hop

		morehops, next, err := current.GetNextHopRPC(id)
		if err != nil {
			// TODO: route error message somewhere?
			badnodes.Add(current)
			continue
		}

		// Try to fetch at every hop
		isRoot, providers, err := current.FetchRPC(key)

		// this must be the root and is thus our final destination
		if !morehops {
			if err != nil {
				return nil, err
			} else if !isRoot {
				return nil, fmt.Errorf("root node did not believe it was the root node")
			} else {
				return providers, nil
			}
		}

		// this node is only one on the path and we will return a list only if providers is not nil
		// this short-circuiting improves speed
		if err == nil {
			if len(providers) != 0 {
				Out.Printf("SHORTCUTTED!")
				return providers, nil
			}
		}

		// Traverse the next node
		nodes = append(nodes, current, next)
		tail += 2
	}

	return nil, fmt.Errorf("unable to get root for %v, all nodes traversed were bad, starting from %v", id, start)
	// __END_TA__
	/* __BEGIN_STUDENT__
	    // TODO: students should implement this
	    return local.node, nil
	__END_STUDENT__ */
}



func salts(key string) []string {
	salts := make([]string, NUM_SALTS)
	for i := 0; i < NUM_SALTS; i++ {
		salts[i] = key
		key = Hash(key).String()
	}
	return salts
}