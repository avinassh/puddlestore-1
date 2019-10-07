package tapestry

import (
	"time"

	. "gopkg.in/check.v1"
)

type MulticastSuite struct{}

var _ = Suite(&MulticastSuite{})

func (o *MulticastSuite) TearDownTest(cc *C) {
	time.Sleep(1 * time.Second)
	closeAllConnections()
}

// Base case multicast check - root node includes itself in neighbor set
func (s *MulticastSuite) TestTA_Multicast1_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, "")
	cc.Assert(err, IsNil)

	neighbors, err := t1.AddNode(t2.node)

	cc.Assert(err, IsNil)
	cc.Assert(hasneighbor(neighbors, t1.node), Equals, true)

	KillTapestries(t1, t2)
}

// Ensure multicast doesn't include nodes that aren't need-to-know nodes
func (s *MulticastSuite) TestTA_Multicast2_Test(cc *C) {
	t1, err := start(MakeID("10"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("20"), 0, t1.node.Address)
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("11"), 0, "")
	cc.Assert(err, IsNil)

	neighbors, err := t1.AddNode(t3.node)

	cc.Assert(err, IsNil)
	cc.Assert(hasneighbor(neighbors, t1.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t2.node), Equals, false)
	cc.Assert(hasneighbor(neighbors, t3.node), Equals, false)

	KillTapestries(t1, t2, t3)
}

// Check that multicast propagates to the next level
func (s *MulticastSuite) TestTA_Multicast3_Test(cc *C) {
	t1, err := start(MakeID("11"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("12"), 0, t1.node.Address)
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("00"), 0, "")
	cc.Assert(err, IsNil)

	neighbors, err := t1.AddNode(t3.node)

	cc.Assert(err, IsNil)
	cc.Assert(hasneighbor(neighbors, t1.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t2.node), Equals, true)

	KillTapestries(t1, t2, t3)
}

// Check that multicast propagates to several levels across multiple nodes
func (s *MulticastSuite) TestTA_Multicast4_Test(cc *C) {
	t1, err := start(MakeID("0000"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("1110"), 0, t1.node.Address)
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("1120"), 0, "")
	cc.Assert(err, IsNil)
	t2.table.Add(t3.node) // ensure that explicitly only t2 knows of t3
	t4, err := start(MakeID("1127"), 0, "")
	cc.Assert(err, IsNil)
	t3.table.Add(t4.node) // ensure that explicitly only t3 knows of t4
	t5, err := start(MakeID("1000"), 0, "")
	cc.Assert(err, IsNil)

	neighbors, err := t2.AddNode(t5.node)

	cc.Assert(err, IsNil)
	cc.Assert(hasneighbor(neighbors, t1.node), Equals, false)
	cc.Assert(hasneighbor(neighbors, t2.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t3.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t4.node), Equals, true)

	KillTapestries(t1, t2, t3, t4, t5)
}

// Check that multicast propagates to several levels within a single node
func (s *MulticastSuite) TestTA_Multicast5_Test(cc *C) {
	t1, err := start(MakeID("0000"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("0100"), 0, "")
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("0010"), 0, "")
	cc.Assert(err, IsNil)
	t4, err := start(MakeID("0001"), 0, "")
	cc.Assert(err, IsNil)
	t1.table.Add(t2.node)
	t1.table.Add(t3.node)
	t1.table.Add(t4.node)
	t5, err := start(MakeID("1000"), 0, "")
	cc.Assert(err, IsNil)

	neighbors, err := t1.AddNode(t5.node)

	cc.Assert(err, IsNil)
	cc.Assert(hasneighbor(neighbors, t1.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t2.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t3.node), Equals, true)
	cc.Assert(hasneighbor(neighbors, t4.node), Equals, true)

	KillTapestries(t1, t2, t3, t4, t5)
}

// Check badnode handling in multicast - ensure badnode not included in neighbourset
func (s *MulticastSuite) TestTA_Multicast6_Test(cc *C) {
	t1, err := start(MakeID("11"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("12"), 0, t1.node.Address)
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("127"), 0, "")
	cc.Assert(err, IsNil)
	badnode := RemoteNode{MakeID("1278"), "asdf"}
	t2.table.Add(t3.node)
	t3.table.Add(badnode)

	t4, err := start(MakeID("0"), 0, "")
	cc.Assert(err, IsNil)
	neighbors, err := t1.AddNode(t4.node)
	cc.Assert(err, IsNil)
	cc.Assert(hasneighbor(neighbors, badnode), Equals, false)

	KillTapestries(t1, t2, t3, t4)
}

// Check badnode handling in multicast (2) - ensure badnode removed from routing table
func (s *MulticastSuite) TestTA_Multicast7_Test(cc *C) {
	t1, err := start(MakeID("11"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("12"), 0, t1.node.Address)
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("127"), 0, "")
	cc.Assert(err, IsNil)
	badnode := RemoteNode{MakeID("1278"), "asdf"}
	t2.table.Add(t3.node)
	t3.table.Add(badnode)

	cc.Assert(hasRoutingTableNode(t3, badnode), Equals, true)

	t4, err := start(MakeID("0"), 0, "")
	cc.Assert(err, IsNil)
	t1.AddNode(t4.node)

	cc.Assert(hasRoutingTableNode(t3, badnode), Equals, false)

	KillTapestries(t1, t2, t3, t4)
}

// Multicasttransfer: object is not transferred when not necessary
func (s *MulticastSuite) TestTA_Multicast8_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t1.Publish("test")

	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(200 * time.Millisecond)

	nodes, err := t2.Lookup("test")

	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 1)
	cc.Assert(nodes[0], Equals, t1.node)

	KillTapestries(t1, t2)
}

// Multicasttransfer: object is transferred when necessary
func (s *MulticastSuite) TestTA_Multicast9_Test(cc *C) {
	t1, err := start(MakeID("2111"), 0, "")
	cc.Assert(err, IsNil)
	t1.Publish("test")

	t2, err := start(MakeID("1111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	time.Sleep(200 * time.Millisecond)

	nodes, err := t2.Lookup("test")

	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 1)
	cc.Assert(nodes[0], Equals, t1.node)

	KillTapestries(t1, t2)
}
