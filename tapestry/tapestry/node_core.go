/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

import (
	"errors"
	"fmt"
	// Uncomment for xtrace
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
	"time"
)

// Store a blob on the local node and publish the key to the tapestry.
func (local *Node) Store(key string, value []byte) (err error) {
	done, err := local.Publish(key)
	if err != nil {
		return err
	}
	Debug.Printf("Data for key %v stored at %v", key, local)
	local.blobstore.Put(key, value, done)
	return nil
}

// Lookup a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *Node) Get(key string) ([]byte, error) {
	// Lookup the key
	replicas, err := local.Lookup(key)
	if err != nil {
		return nil, err
	}
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no nodes found during lookup of key %v", key)
	}

	// Contact replicas
	var errs []error
	for _, replica := range replicas {
		blob, err := replica.BlobStoreFetchRPC(key)
		if err != nil {
			errs = append(errs, err)
		}
		if blob != nil {
			return *blob, nil
		}
	}

	return nil, fmt.Errorf("Error contacting replicas, %v: %v", replicas, errs)
}

// Remove the blob from the local blob store and stop advertising
func (local *Node) Remove(key string) bool {
	return local.blobstore.Delete(key)
}

// STUDENT WRITTEN
// Periodically publishes the given key by navigating to the
// root node for the key (it could change between publishes).
// If something is sent on the done channel, cancels publishing
// and returns.
func (local *Node) periodicallyPublish(key string, keyID ID, done chan bool) {
	tick := time.NewTicker(REPUBLISH)
	for {
		// take the first RX one either the done channel
		// or our tick channel for republishes
		select {
		case <-done:
			return
		case <-tick.C:
			// on each timer tick, re-publish to the node as we initially did
			err := local.publishOnce(key, keyID)
			if err != nil {
				Error.Printf("Periodic publish of %s errored: %s", key, err)
			} else {
				Debug.Printf("Re-publishing key %s\n", key)
			}
			// ignore publish errors and try again next time
		}
	}
}

// STUDENT WRITTEN
// Publishes the given key (in both forms) to its root,
// computing which node that root is each time
func (local *Node) publishOnce(key string, keyID ID) (err error) {
	// find root node (id with closest id to given id)
	// (start from this node (use our routing table))
	root, err, _, _ := local.findRoot(local.node, keyID)
	if err != nil {
		return err
	}

	// register us on the root node as a storage location for the given key
	ok, err := root.RegisterRPC(key, local.node)
	if !ok && err == nil { // favor returning err
		return errors.New("register RPC did not return OK, but gave no error")
	}
	return err
}

// STUDENT WRITTEN
// Publishes the key in tapestry.
//
// - Route to the root node for the key
// - Register our local node on the root
// - Start periodically republishing the key
// - Return a channel for cancelling the publish
func (local *Node) Publish(key string) (done chan bool, err error) {
	Out.Printf("Publishing key %s at %v\n", key, local)

	// get hash of key to use as ID
	keyID := Hash(key)

	err = local.publishOnce(key, keyID)
	if err != nil {
		Debug.Printf("Initial object publish failed: %v", err)
		return nil, err
	}

	// create channel for indicating done, and then create a goroutine which
	// will publish until "done" is sent on that channel
	done = make(chan bool)
	go local.periodicallyPublish(key, keyID, done)

	return done, nil
}

// STUDENT WRITTEN
// Look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the replicas (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *Node) Lookup(key string) (nodes []RemoteNode, err error) {
	// get hash of key to use as ID
	keyID := Hash(key)

	// don't set error until end because we may get it on retry
	var possibleErr error = nil

	// keep retrying if we encounter errors (findRoot will notify people of issues)
	for try := 0; try < RETRIES; try++ {
		// find root node (id with closest id to given id)
		// (start from this node (use our routing table))
		root, possibleErr, _, _ := local.findRoot(local.node, keyID)
		if possibleErr != nil {
			Debug.Printf("Unreachable node notice: findRoot failed at %v", keyID)
			continue // try again
		} else {
			Debug.Printf("Lookup query for %v (%v) routed to root %v", key, keyID, root)
		}

		// call RPC to get the locations registered on the root node which
		// store the value for our key
		ok, nodes, possibleErr := root.FetchRPC(key)
		if ok && possibleErr == nil {
			Debug.Printf("Fetch %v (%v) succeeded from %v", key, keyID, root)
			return nodes, nil
		} else if !ok {
			possibleErr = errors.New("fetch RPC did not return OK, but gave no error")
		} else { // err != nil
			Debug.Printf("Unreachable node notice: fetch failed at %v, reason: %v", root, err)
		}
		// continue on any other error
	}
	// try timed out
	return nil, possibleErr
}

// STUDENT WRITTEN
// Returns the best candidate from our routing table for routing to the provided ID
// Returns that there are no morehops if an error occured
func (local *Node) GetNextHop(id ID) (morehops bool, nexthop RemoteNode, err error) {
	err = nil
	nexthop = local.table.GetNextHop(id)
	// no more hops if next hop is local node
	morehops = !nexthop.Equals(local.node)
	return
}

// STUDENT WRITTEN
// returns whether we're the root of the given key,
// AND produces an error if we weren't
func (local *Node) checkIfRootOf(key string) (isRoot bool, err error) {
	// get next hop to determine if we're the root
	moreHops, _, err := local.GetNextHop(Hash(key))
	isRoot = !moreHops
	if !isRoot && err == nil {
		return false, errors.New(fmt.Sprintf("not a root for key %s when expected to be", key))
	} else if err != nil {
		return isRoot, err
	}
	return isRoot, nil
}

// STUDENT WRITTEN
// Register the specified node as an advertiser of the specified key.
// - Check that we are the root node for the key
// - Add the node to the location map
// - Kick off a timer to remove the node if it's not advertised again after a set amount of time
func (local *Node) Register(key string, replica RemoteNode) (isRoot bool, err error) {
	isRoot, err = local.checkIfRootOf(key)
	PrintIfError(err, "Root check failed for %s", key)
	if err != nil {
		return isRoot, err
	}

	// if we're the root, add the given node to our location map for this key
	// (this also starts a timer to remove it if not re-registered)
	existed := local.locationsByKey.Register(key, replica, TIMEOUT)

	// debug
	prefix := ""
	if existed {
		prefix = "re-"
	}
	Debug.Printf("Node %v %sregistered as root of key %s\n", local, prefix, key)
	Debug.Printf("%v: replica %v of key %s %sregistered here\n", local.node, replica, key, prefix)

	return
}

// STUDENT WRITTEN
// - Check that we are the root node for the requested key
// - Return all nodes that are registered in the local location map for this key
func (local *Node) Fetch(key string) (isRoot bool, replicas []RemoteNode, err error) {
	isRoot, err = local.checkIfRootOf(key)
	PrintIfError(err, "Root check failed for %s", key)
	if err != nil {
		return isRoot, nil, err
	}
	replicas = local.locationsByKey.Get(key)
	return // err must be nil here
}

// STUDENT WRITTEN
// - Register all of the provided objects in the local location map
// - If appropriate, add the from node to our local routing table
func (local *Node) Transfer(from RemoteNode, replicamap map[string][]RemoteNode) (err error) {
	Out.Printf("Transfering %d replicas to this node from %v", len(replicamap), from)
	// register all replicas here, and start timeout timers
	local.locationsByKey.RegisterAll(replicamap, TIMEOUT)

	// add source node to our routing table if it should be
	local.addRoute(from)
	return nil
}

// STUDENT WRITTEN
// Utility function for iteratively contacting nodes to get the root node for the provided ID.
//
// -  Starting from the specified node, iteratively contact nodes calling getNextHop until we reach the root node
// -  Also keep track of any bad nodes that errored during lookup
// -  At each step, notify the next-hop node of all of the bad nodes we have encountered along the way
// Returns several extra parameters for testing (#hops and hops sequence)
func (local *Node) findRoot(start RemoteNode, id ID) (RemoteNode, error, int, []RemoteNode) {
	Debug.Printf("Routing to %v\n", id)

	// keep list of previously-visited nodes to fall back on if
	// we encounter dead ends due to failures
	previousNodes := make([]RemoteNode, 0)

	// keep track of the bad nodes (those we couldn't connect to),
	// so we can let the nodes we encounter know about them as we traverse
	// (in particular, we use these to remove badNodes from higher-level
	//  nodes to allow us to traverse a different path)
	badNodes := make([]RemoteNode, 0)
	curNode := start
	previousNodes = append(previousNodes, curNode) // initial

	hops := 0
	for hops < MAX_HOPS {
		// get the possible next hop from the current hop node
		morehops, nextHop, err := curNode.GetNextHopRPC(id)
		hops++

		// if the current node failed to get the next hop, add it to our bad nodes,
		// and begin traversing upwards to find a new path
		if err != nil {
			badNodes = append(badNodes, curNode)
			// keep traversing upwards through our previousNodes
			// until we either find a new path or run out of them
			// (there's always at least curNode in previousNodes)
			if len(previousNodes) > 1 {
				// remove "current" curNode, then get previous one
				previousNodes = previousNodes[:len(previousNodes)-1]
				hops--
				curNode = previousNodes[len(previousNodes)-1]

			} else {
				// start is just to signify error
				return start, fmt.Errorf("failed to find root because couldn't find path around bad node (or other err): %v", err), hops, previousNodes
			}
			// remove the bad nodes (including the one we just saw)
			// from the next node we're going to try (so we should get a new
			// option from the extra entries in the routing table next time around)
			err := curNode.RemoveBadNodesRPC(badNodes)
			if err != nil {
				Debug.Printf("Unreachable node notice: couldn't remove bad nodes from %v: %v", curNode, err)
			}

		} else {
			// terminate if we've reached the root (the curNode (previous next hop) was the root)
			if !morehops {
				return curNode, nil, hops, previousNodes
			}

			// let every node know about the bad nodes we've found along the way
			err := curNode.RemoveBadNodesRPC(badNodes)
			if err != nil {
				Debug.Printf("Unreachable node notice: couldn't remove bad nodes from %v: %v", curNode, err)
			}

			// move on to consider next hop, adding that to our previousNodes preemptively
			curNode = nextHop
			previousNodes = append(previousNodes, curNode) // curNode == nextHop
		}
	}
	err := fmt.Errorf("reached max hops trying to find root of %v", id)
	Error.Print(err)
	return start, err, hops, previousNodes
}
