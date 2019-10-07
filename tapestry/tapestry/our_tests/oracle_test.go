package tapestry

import (
	"testing"
)

func TestAdding(t *testing.T) {
	tp := NewTapestry()
	j := 10
	for i := 0; i < j; i++ {
		RunCommands(t, tp, []int{ADD})
	}
	for i := 0; i < j; i++ {
		Out.Printf(tp.Nodes[i].RoutingTableToString())
	}
}

func TestSimpleTransaction(t *testing.T) {
	tp := NewTapestry()
	RunCommands(t, tp, []int{ADD, PUB, GET})
}

func TestAddingAndLeaving(t *testing.T) {
	tp := NewTapestry()
	RunCommands(t, tp, []int{ADD, ADD, ADD, ADD, LEAVE, LEAVE, LEAVE, LEAVE, LEAVE, ADD, ADD, LEAVE, ADD, ADD})
}

func TestAddingAndKilling(t *testing.T) {
	tp := NewTapestry()
	RunCommands(t, tp, []int{ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD, ADD,
		ADD, ADD, ADD})
	RunCommands(t, tp, []int{KILL, KILL, ADD, KILL, ADD, ADD, KILL, KILL, KILL, KILL, KILL, ADD})
}

// TestTransactionMultipleNodes tests a simple transaction
// on a large number of nodes
func TestTransactionMultipleNodes(t *testing.T) {
	tp := NewTapestry()
	for i := 0; i < 100; i++ {
		RunCommands(t, tp, []int{ADD})
	}
	RunCommands(t, tp, []int{PUB, GET})
}

// TestMultipleTransactionMultipleNodes tests a random sequence
// of ADD/PUB/GET
func TestMultipleTransactionMultipleNodes(t *testing.T) {
	tp := NewTapestry()
	// start with ten nodes
	for i := 0; i < 10; i++ {
		RunCommands(t, tp, []int{ADD})
	}
	// execute 200 random adds, pubs and gets
	opts := []int{ADD, PUB, GET}
	for i := 0; i < 200; i++ {
		RunCommands(t, tp, []int{opts[tp.Rand.Intn(3)]})
	}
}

// TestTransactionPostLeave tests a simple single transaction after
// a network has nodes added and left
func TestTransactionPostLeave(t *testing.T) {
	tp := NewTapestry()
	RunCommands(t, tp, []int{ADD, ADD, ADD, LEAVE, LEAVE, ADD, ADD, ADD, ADD})
	RunCommands(t, tp, []int{PUB, GET})
}

// randomized version of the above
func TestRandomizedTransactionPostLeave(t *testing.T) {
	opts := []int{ADD, LEAVE}
	tp := NewTapestry()
	for i := 0; i < 100; i++ {
		RunCommands(t, tp, []int{opts[tp.Rand.Intn(len(opts))]})
	}
	for i := 0; i < 50; i++ {
		RunCommands(t, tp, []int{PUB, GET})
	}
}

// Tests whether a node (possibly the root) leaving affects get
func TestSimpleLeavePostTransaction(t *testing.T) {
	SetDebug(false)
	tp := NewTapestry()
	RunCommands(t, tp, []int{ADD, ADD, ADD, ADD, PUB, LEAVE, LEAVE, GET})
	for _, n := range tp.Nodes {
		Out.Printf(n.RoutingTableToString())
	}
}

// Randomly select ADD, PUB, GET, LEAVE
func TestRandomizedLeavePostTransaction(t *testing.T) {
	tp := NewTapestry()
	opts := []int{ADD, PUB, GET, LEAVE}
	for i := 0; i < 250; i++ {
		RunCommands(t, tp, []int{opts[tp.Rand.Intn(3)]})
	}
}

func TestTransactionPostKill(t *testing.T) {
	tp := NewTapestry()
	RunCommands(t, tp, []int{ADD, ADD, ADD, KILL, KILL, ADD, ADD, ADD, ADD, KILL, KILL, KILL, PUB, GET})
}

func TestRandomSequence(t *testing.T) {
	tp := NewTapestry()
	RunCommands(t, tp, GenerateCommands(250))
}
