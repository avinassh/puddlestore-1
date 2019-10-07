package tapestry

import (
	"time"

	. "gopkg.in/check.v1"
)

type BPTraversalSuite struct{}

var _ = Suite(&BPTraversalSuite{})

func (o *BPTraversalSuite) TearDownTest(cc *C) {
	time.Sleep(1 * time.Second)
	closeAllConnections()
}

// 4 node neighbor set test
// Check backpointer traversel with iterative additions
func (s *BPTraversalSuite) TestTA_TapestryBPTraversal1_Test(cc *C) {
	ts, err := MakeTapestries(true, "00000", "00001", "00010", "00100", "01000", "10000")
	cc.Assert(err, IsNil)
	tn, err := start(MakeID("000011"), 0, ts[1].node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < len(ts); i++ {
		cc.Assert(tn.table.Contains(ts[i].node), Equals, true)
	}

	for i := 0; i < len(ts); i++ {
		ts[i].Kill()
	}
	tn.Kill()
}

// Start two nodes, don't connect them, manually add a backpointer.
// Start a new node, check the manual backpointer is included.
func (s *BPTraversalSuite) TestTA_TapestryBPTraversal2_Test(cc *C) {
	t1, err := start(MakeID("10"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("20"), 0, "")
	cc.Assert(err, IsNil)

	t2.backpointers.Add(t1.node)

	t3, err := start(MakeID("21"), 0, t2.node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(100 * time.Millisecond)

	// Check BP was added due to RT entry
	cc.Assert(hasBackpointer(t1, t3), Equals, true)

	t1.Kill()
	t2.Kill()
	t3.Kill()
}

// Start two nodes, don't connect them, manually add a backpointer.
// Start a new node, check the manual backpointer is included.
func (s *BPTraversalSuite) TestTA_TapestryBPTraversal3_Test(cc *C) {
	t1, err := start(MakeID("10"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("20"), 0, "")
	cc.Assert(err, IsNil)

	t2.backpointers.Add(t1.node)

	t3, err := start(MakeID("21"), 0, t2.node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(100 * time.Millisecond)

	// Check RT entry was added by BP traversal
	cc.Assert(hasRoutingTableEntry(t3, t1), Equals, true)

	t1.Kill()
	t2.Kill()
	t3.Kill()
}

// Start two nodes, don't connect them, manually add a backpointer.
// Start a new node, check the manual backpointer is included.
func (s *BPTraversalSuite) TestTA_TapestryBPTraversal4_Test(cc *C) {
	t1, err := start(MakeID("10"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("20"), 0, "")
	cc.Assert(err, IsNil)

	t2.backpointers.Add(t1.node)

	t3, err := start(MakeID("21"), 0, t2.node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(100 * time.Millisecond)

	// Check RT entry was not added to t2
	cc.Assert(hasRoutingTableEntry(t2, t1), Equals, false)

	t1.Kill()
	t2.Kill()
	t3.Kill()
}

// Start two nodes, don't connect them, manually add a backpointer many layers deep.
// Start a new node, check the manual backpointer is included.
func (s *BPTraversalSuite) TestTA_TapestryBPTraversal5_Test(cc *C) {
	t1, err := start(MakeID("10"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2000"), 0, "")
	cc.Assert(err, IsNil)

	t2.backpointers.Add(t1.node)

	t3, err := start(MakeID("2001"), 0, t2.node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(100 * time.Millisecond)

	// Check RT entry was not added to t2
	cc.Assert(hasRoutingTableEntry(t2, t1), Equals, false)

	t1.Kill()
	t2.Kill()
	t3.Kill()
}
