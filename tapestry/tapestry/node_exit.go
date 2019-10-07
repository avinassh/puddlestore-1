/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package tapestry

// Uncomment for xtrace
// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"

// Kill this node without gracefully leaving the tapestry.
func (local *Node) Kill() {
	local.blobstore.DeleteAll()
	local.server.Stop()
}

// STUDENT WRITTEN
// Finds a suitable replacement for the local node in the Tapestry network,
// based on the contents of our routing table
func (local *Node) getSelfReplacement() (replacement *RemoteNode) {
	// starting at highest (highest-similarity) level, try and find
	// the nodes that are most similar to us in the Tapestry network
	// using the BetterChoice metric
	// (we'll only move on to a lower level if there are no options
	//  in the current level)
	for level := DIGITS - 1; level >= 0; level-- {
		nodesHere := local.table.GetLevel(level)
		if len(nodesHere) > 0 {
			// if some exist, find the "best choice" relative to us
			bestReplacement := nodesHere[0]
			for _, node := range nodesHere {
				if local.node.Id.BetterChoice(node.Id, bestReplacement.Id) {
					bestReplacement = node
				}
			}
			return &bestReplacement // go's good with garbage
		}
		// if none exist, try next level
	}
	// if we have no node options at all (?), return nil
	return nil
}

// STUDENT WRITTEN
// Function to gracefully exit the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *Node) Leave() (err error) {
	Debug.Printf("Performing graceful exit of Tapestry")
	// notify all backpointers at each level
	localReplacement := local.getSelfReplacement()
	for level := 0; level < DIGITS; level++ {
		for _, node := range local.backpointers.Get(level) {
			err = node.NotifyLeaveRPC(local.node, localReplacement)
			if err != nil {
				Debug.Printf("Unreachable node notice: couldn't notify %v of us leaving: ", node)
			}
		}
	}

	local.blobstore.DeleteAll()
	go local.server.GracefulStop()
	return
}

// STUDENT WRITTEN
// Another node is informing us of a graceful exit.
// - Remove references to the from node from our routing table and backpointers
// - If replacement is not nil, add replacement to our routing table
func (local *Node) NotifyLeave(from RemoteNode, replacement *RemoteNode) (err error) {
	Debug.Printf("%v: received leave notification from %v with replacement node %v\n", local.node, from, replacement)

	// remove the node that is leaving from both our routing table
	// (it may not be there if there's better nodes near us)
	// and backpointers (if we happened to be mutual backpointers)
	local.table.Remove(from)
	local.backpointers.Remove(from)

	// add replacement if there was one
	if replacement != nil {
		local.addRoute(*replacement)
	}

	return
}
