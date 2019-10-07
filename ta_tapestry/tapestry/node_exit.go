/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package tapestry

// __BEGIN_TA__
// import (
// 	xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
// )

// __END_TA__
/* __BEGIN_STUDENT__
import (
	// Uncomment for xtrace
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)
__END_STUDENT__ */

// Kill this node without gracefully leaving the tapestry.
func (local *Node) Kill() {
	local.blobstore.DeleteAll()
	local.server.Stop()
}

// Function to gracefully exit the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *Node) Leave() (err error) {
	// __BEGIN_TA__

	Debug.Println("Leaving tapestry")
	// xtr.NewTask("leave")
	var replacement *RemoteNode
	for i := DIGITS - 1; i >= 0; i-- {
		// Get the backpointers for level i
		backpointers := local.backpointers.Get(i)
		done := make(chan bool)

		// Asynchronously notify the backpointers of the leave
		for _, node := range backpointers {
			node := node
			go func() {
				// Notify the node of the leave
				err := node.NotifyLeaveRPC(local.node, replacement)

				// Remove the bad node so we don't select it as the replacement
				if err != nil {
					local.RemoveBadNodes([]RemoteNode{node})
				}
				// xtr.SendChannelEvent(done)
				done <- true
			}()
		}

		// Await completion
		for _, _ = range backpointers {
			<-done
			// xtr.ReadChannelEvent(done)
		}
		Trace.Println("finished adding backpointers")

		// Get a replacement from routing table level i, that we will give to backpointers at level i-1
		routinglevel := local.table.GetLevel(i)

		if len(routinglevel) > 0 {
			replacement = &(routinglevel[0])
		} else {
			replacement = nil
		}
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	local.blobstore.DeleteAll()
	go local.server.GracefulStop()
	return
}

// Another node is informing us of a graceful exit.
// - Remove references to the from node from our routing table and backpointers
// - If replacement is not nil, add replacement to our routing table
func (local *Node) NotifyLeave(from RemoteNode, replacement *RemoteNode) (err error) {
	Debug.Printf("Received leave notification from %v with replacement node %v\n", from, replacement)

	// __BEGIN_TA__
	if local.table.Remove(from) {
		Debug.Printf("Removed %v from routing table\n", from)
	}
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed %v from backpointers\n", from)
	}
	if replacement != nil {
		err = local.addRoute(*replacement)
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}
