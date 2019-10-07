/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"bytes"
	"strconv"
	"sync"
)

// A routing table has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table protected by a mutex.
type RoutingTable struct {
	local RemoteNode                  // The local tapestry node
	rows  [DIGITS][BASE]*[]RemoteNode // The rows of the routing table
	mutex sync.Mutex                  // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// Creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me RemoteNode) *RoutingTable {
	t := new(RoutingTable)
	t.local = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			slot := make([]RemoteNode, 0, SLOTSIZE)
			t.rows[i][j] = &slot
		}
	}

	// Make sure each row has at least our node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.rows[i][t.local.Id[i]]
		*slot = append(*slot, t.local)
	}

	return t
}

// STUDENT WRITTEN - TESTED
// Adds the given node to the routing table.
// If node previously existed in the routing table => false, nil
// If node did not previously exist in the table and was added => true, nil
// Returns the previous node in the table, if one was overwritten. Adds to the row furthest
// that it is allowed to add to
func (t *RoutingTable) Add(node RemoteNode) (added bool, previous *RemoteNode) {

	t.mutex.Lock()

	added = false
	previous = nil

	// we want to add to the ith row of the routing table
	i := SharedPrefixLength(t.local.Id, node.Id)

	if i == DIGITS {
		// is the same as the current node - should already be present
		t.mutex.Unlock()
		return
	}

	// extracting all the values that would otherwise be in the spot
	slot := t.rows[i][node.Id[i]]

	// if node is already contained, do not add in
	for _, n := range *slot {
		if node.Equals(n) {
			t.mutex.Unlock()
			return
		}
	}

	// slot is full
	if len(*slot) >= SLOTSIZE {
		furthest := (*slot)[0]
		furthestIndex := 0
		for i, n := range *slot {
			if t.local.Id.Closer(furthest.Id, n.Id) {
				furthest = n
				furthestIndex = i
			}
		}
		if t.local.Id.Closer(node.Id, furthest.Id) {
			// if new node is closer, add it
			(*slot)[furthestIndex] = node
			added = true
			previous = &furthest
		}
	} else {
		// slot not full, so append normally
		*slot = append(*slot, node)
		added = true
	}

	t.mutex.Unlock()

	return
}

// STUDENT WRITTEN
// Removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
func (t *RoutingTable) Remove(node RemoteNode) (wasRemoved bool) {

	t.mutex.Lock()

	wasRemoved = false

	// extracts items to search in
	i := SharedPrefixLength(t.local.Id, node.Id)
	vals := t.rows[i][node.Id[i]]

	for i, n := range *vals {
		if node.Id.String() == n.Id.String() {
			// removes value
			*vals = append((*vals)[:i], (*vals)[i+1:]...)
			wasRemoved = true
		}
	}

	t.mutex.Unlock()

	return
}

// STUDENT WRITTEN -  TESTED
// Get all nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodes []RemoteNode) {

	t.mutex.Lock()

	// allocates enough space
	nodes = make([]RemoteNode, 0)
	for i := 0; i < BASE; i++ {
		for _, n := range *t.rows[level][i] {
			if n.Id.String() != t.local.Id.String() {
				// adds to nodes
				nodes = append(nodes, n)
			}
		}
	}

	t.mutex.Unlock()

	return
}

// STUDENT WRITTEN
// GetBestNodeInSlot returns pointer to the best node in slot, and nil if
// no nodes in the slot.
// SHOULD BE CALLED AS A PART OF ATOMIC OPERATIONS - IF STANDALONE CALL, WRAP WITH LOCKS
func (t *RoutingTable) GetBestNodeInSlot(id ID, slot []RemoteNode) (best *RemoteNode) {
	best = nil
	for i, n := range slot {
		// updates best according to best choice
		if best == nil || id.BetterChoice(n.Id, (*best).Id) {
			best = &slot[i]
		}
	}
	return
}

// STUDENT WRITTEN
// Search the table for the closest next-hop node for the provided ID.
// *MUST* return local node if it is the root for the given ID
func (t *RoutingTable) GetNextHop(id ID) (node RemoteNode) {

	t.mutex.Lock()

	// traverse rows starting from that of max prefix length matching
	for i := SharedPrefixLength(t.local.Id, id); i < DIGITS; i++ {
		// index of column we want to start searching from
		j := id[i]
		// traverse entire row starting from j
		for ok := true; ok; ok = j != id[i] {
			slot := *(t.rows[i][j])

			best := t.GetBestNodeInSlot(id, slot)

			//if there is some best node
			if best != nil {
				if best.Id.String() == t.local.Id.String() {
					// best node is the root node, so we break and go to the next level
					break
				} else {
					// we have found some node that is better than the root node so we return
					t.mutex.Unlock()
					return *best
				}
			}

			// we found nothing in current slot so we go to next slot
			// incremement j to move to the next slot with wraparound
			j = (j + 1) % BASE
		}
	}

	// there are no other better nodes in the table so the current node holds the object!
	t.mutex.Unlock()
	return t.local
}

func (t *RoutingTable) String() string {
	var buffer bytes.Buffer

	buffer.WriteString("Routing table for " + t.local.Id.String() + ":\n")
	for i := 0; i < DIGITS; i++ {
		buffer.WriteString("row " + strconv.Itoa(i) + ": ")
		for _, node := range t.GetLevel(i) {
			buffer.WriteString(node.Id.String() + ", ")
		}
		buffer.WriteString("\n")
	}

	return buffer.String()
}
