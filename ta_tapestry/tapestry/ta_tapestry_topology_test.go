package tapestry

import (
	"time"

	. "gopkg.in/check.v1"
)

type TopologySuite struct{}

var _ = Suite(&TopologySuite{})

func (o *TopologySuite) TearDownTest(cc *C) {
	time.Sleep(1 * time.Second)
	closeAllConnections()
}

func (b *TopologySuite) SetUpTest(cc *C) {
	time.Sleep(1 * time.Second)
}

// 1 node routing table check
func (s *TopologySuite) TestTA_Join1_RoutingTable_Test(cc *C) {
	t1, err := MakeOne("11")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	cc.Assert(hasRoutingTableEntry(t1, t1), Equals, true)
}

// 2 node tapestry join, ensure routing table on each node contains other
func (s *TopologySuite) TestTA_Join2_RoutingTable_Test(cc *C) {
	t1, t2, err := MakeTwo("1", "2")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	cc.Assert(hasRoutingTableEntry(t1, t2), Equals, true)
	cc.Assert(hasRoutingTableEntry(t2, t1), Equals, true)
}

// 2 node tapestry join after kill
func (s *TopologySuite) TestTA_BadJoin2_Test(cc *C) {
	t1, err := MakeOne("1")
	cc.Assert(err, IsNil)

	addr := t1.node.Address
	KillTapestries(t1)

	t2, err := start(MakeID("2"), 0, addr)
	cc.Assert(err, NotNil)
	cc.Assert(t2, IsNil)
}

// 3 node tapestry join
func (s *TopologySuite) TestTA_Join3_RoutingTable_Test(cc *C) {
	t1, t2, t3, err := MakeThree("1", "2", "3")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2, t3)

	cc.Assert(hasRoutingTableEntry(t1, t2), Equals, true)
	cc.Assert(hasRoutingTableEntry(t1, t3), Equals, true)
	cc.Assert(hasRoutingTableEntry(t2, t1), Equals, true)
	cc.Assert(hasRoutingTableEntry(t2, t3), Equals, true)
	cc.Assert(hasRoutingTableEntry(t3, t1), Equals, true)
	cc.Assert(hasRoutingTableEntry(t3, t2), Equals, true)
}

// Simple backpointers test with 1 nodes
func (s *TopologySuite) TestTA_Join1_Backpointers_Test(cc *C) {
	t1, err := MakeOne("1")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	cc.Assert(hasBackpointer(t1, t1), Equals, false)
}

// Simple backpointers test with 2 nodes, check root adds backpointer for new node
func (s *TopologySuite) TestTA_Join2_Backpointers_Test(cc *C) {
	t1, t2, err := MakeTwo("11", "12")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	TapestryPause()

	cc.Assert(hasBackpointer(t1, t2), Equals, true)
	cc.Assert(hasBackpointer(t2, t1), Equals, true)
}

// Start enough nodes to have more than SLOTSIZE nodes in a RT slot,
// ensure the backpointers don't exist
func (s *TopologySuite) TestTA_Join5_Backpointers_Test(cc *C) {
	ts, err := MakeTapestries(true, "11", "20", "21", "22", "23")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	for i := 0; i < len(ts); i++ {
		for j := 0; j < len(ts); j++ {
			if i == 4 && j == 0 {
				cc.Assert(hasBackpointer(ts[i], ts[j]), Equals, false)
			} else if i != j {
				cc.Assert(hasBackpointer(ts[i], ts[j]), Equals, true)
			}
		}
	}
}

// Test notify leave in a 1 node tapestry with a dummy node
func (s *TopologySuite) TestTA_NotifyLeave1_Test(cc *C) {
	t1, err := MakeOne("1")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	leaving := RemoteNode{MakeID("2"), "asdf"}

	t1.table.Add(leaving)
	t1.backpointers.Add(leaving)

	cc.Assert(hasRoutingTableNode(t1, leaving), Equals, true)
	cc.Assert(hasBackpointerNode(t1, leaving), Equals, true)

	err = t1.NotifyLeave(leaving, nil)

	TapestryPause()

	cc.Assert(err, IsNil)
	cc.Assert(hasRoutingTableNode(t1, leaving), Equals, false)
	cc.Assert(hasBackpointerNode(t1, leaving), Equals, false)

}

// Test notify leave in a 2 node tapestry with a replacement node
func (s *TopologySuite) TestTA_NotifyLeave2_Test(cc *C) {
	t1, err := MakeOne("1")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	t2, err := MakeOne("22")
	cc.Assert(err, IsNil)
	defer KillTapestries(t2)

	leaving := RemoteNode{MakeID("2"), "asdf"}
	replacement := t2.node

	t1.table.Add(leaving)
	t1.backpointers.Add(leaving)

	cc.Assert(hasRoutingTableNode(t1, leaving), Equals, true)
	cc.Assert(hasRoutingTableNode(t1, replacement), Equals, false)

	err = t1.NotifyLeave(leaving, &replacement)

	TapestryPause()

	cc.Assert(err, IsNil)
	cc.Assert(hasRoutingTableNode(t1, leaving), Equals, false)
	cc.Assert(hasRoutingTableNode(t1, replacement), Equals, true)
}

// Test add route adds to routing table as appropriate
func (s *TopologySuite) TestTA_AddRoute2_RoutingTable_Test(cc *C) {
	t1, err := MakeOne("1")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	t2, err := start(MakeID("2"), 0, "")
	cc.Assert(err, IsNil)
	defer KillTapestries(t2)

	err = t1.addRoute(t2.node)

	TapestryPause()

	cc.Assert(err, IsNil)
	cc.Assert(hasRoutingTableEntry(t1, t2), Equals, true)
}

// Test add route notifies backpointer
func (s *TopologySuite) TestTA_AddRoute2_Backpointer_Test(cc *C) {
	t1, err := MakeOne("1")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	t2, err := start(MakeID("2"), 0, "")
	cc.Assert(err, IsNil)
	defer KillTapestries(t2)

	err = t1.addRoute(t2.node)

	TapestryPause()

	cc.Assert(err, IsNil)
	cc.Assert(hasBackpointer(t2, t1), Equals, true)
}

// Test add route evicts a previous node if it's better
func (s *TopologySuite) TestTA_AddRoute5_Eviction_1_Test(cc *C) {
	ts, err := MakeTapestries(true, "1", "241", "242", "243")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	for i := 1; i < len(ts); i++ {
		cc.Assert(hasRoutingTableEntry(ts[0], ts[i]), Equals, true)
	}

	nt, err := MakeOne("200")
	cc.Assert(err, IsNil)
	defer KillTapestries(nt)

	ts = append(ts, nt)
	ts[0].addRoute(ts[4].node)

	TapestryPause()

	cc.Assert(hasRoutingTableEntry(ts[0], ts[1]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[0], ts[2]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[0], ts[3]), Equals, false)
	cc.Assert(hasRoutingTableEntry(ts[0], ts[4]), Equals, true)
}

// Test add route does not evict a previous node if it's worse
func (s *TopologySuite) TestTA_AddRoute5_Eviction_2_Test(cc *C) {
	ts, err := MakeTapestries(true, "1", "241", "242", "243")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	for i := 1; i < len(ts); i++ {
		cc.Assert(hasRoutingTableEntry(ts[0], ts[i]), Equals, true)
	}

	nt, err := MakeOne("244")
	cc.Assert(err, IsNil)
	defer KillTapestries(nt)

	ts = append(ts, nt)
	ts[0].addRoute(ts[4].node)

	TapestryPause()

	cc.Assert(hasRoutingTableEntry(ts[0], ts[1]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[0], ts[2]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[0], ts[3]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[0], ts[4]), Equals, false)
}

// Test add route notifies previous node of removal on success
func (s *TopologySuite) TestTA_AddRoute5_Eviction_Notification_Test(cc *C) {
	ts, err := MakeTapestries(true, "1", "241", "242", "243")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	for i := 1; i < len(ts); i++ {
		cc.Assert(hasRoutingTableEntry(ts[0], ts[i]), Equals, true)
	}

	nt, err := MakeOne("200")
	cc.Assert(err, IsNil)
	defer KillTapestries(nt)

	ts = append(ts, nt)
	ts[0].addRoute(ts[4].node)

	TapestryPause()

	cc.Assert(hasBackpointer(ts[1], ts[0]), Equals, true)
	cc.Assert(hasBackpointer(ts[2], ts[0]), Equals, true)
	cc.Assert(hasBackpointer(ts[3], ts[0]), Equals, false)
	cc.Assert(hasBackpointer(ts[4], ts[0]), Equals, true)
}

// Test add route retains previous node if cannot notify new node of backpointer
func (s *TopologySuite) TestTA_AddRoute5_Eviction_Notification_Failure_Test(cc *C) {
	ts, err := MakeTapestries(true, "1", "241", "242", "243")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	for i := 1; i < len(ts); i++ {
		cc.Assert(hasRoutingTableEntry(ts[0], ts[i]), Equals, true)
		cc.Assert(hasBackpointer(ts[i], ts[0]), Equals, true)
	}

	n5 := RemoteNode{MakeID("200"), "asdf"}

	ts[0].addRoute(n5)

	TapestryPause()

	for i := 1; i < len(ts); i++ {
		cc.Assert(hasRoutingTableEntry(ts[0], ts[i]), Equals, true)
		cc.Assert(hasBackpointer(ts[i], ts[0]), Equals, true)
	}
	cc.Assert(hasRoutingTableNode(ts[0], n5), Equals, false)
}

// Test register returns true if we are the root
func (s *TopologySuite) TestTA_Register1_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	isRoot, err := t1.Register("test", t1.node)

	cc.Assert(err, IsNil)
	cc.Assert(isRoot, Equals, true)
}

// Test register returns false if we are not the root
func (s *TopologySuite) TestTA_Register2_Test(cc *C) {
	t1, t2, err := MakeTwo("1", "2")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	isRoot, err := t2.Register("test", t1.node)

	cc.Assert(err, IsNil)
	cc.Assert(isRoot, Equals, false)
}

// Test register returns false if we are the root
func (s *TopologySuite) TestTA_Register3_Test(cc *C) {
	t1, t2, err := MakeTwo("1", "2")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	isRoot, err := t1.Register("test", t1.node)

	cc.Assert(err, IsNil)
	cc.Assert(isRoot, Equals, true)
}

// getnexthop: morehops = false when appropriate
func (s *TopologySuite) TestTA_GetNextHop1_1_Test(cc *C) {
	t1, err := MakeOne("77")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	morehops, nexthop, err := t1.GetNextHop(MakeID("77"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, false)
	cc.Assert(nexthop, Equals, t1.node)
}

// getnexthop: morehops = false when appropriate (2)
func (s *TopologySuite) TestTA_GetNextHop1_2_Test(cc *C) {
	t1, err := MakeOne("77")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	morehops, nexthop, err := t1.GetNextHop(MakeID("20348ADFE"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, false)
	cc.Assert(nexthop, Equals, t1.node)
}

// getnexthop: morehops = true when appropriate
func (s *TopologySuite) TestTA_GetNextHop2_1_Test(cc *C) {
	t1, t2, err := MakeTwo("77", "88")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	morehops, nexthop, err := t1.GetNextHop(MakeID("77"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, false)
	cc.Assert(nexthop, Equals, t1.node)
}

// getnexthop: morehops = true when appropriate
func (s *TopologySuite) TestTA_GetNextHop2_2_Test(cc *C) {
	t1, t2, err := MakeTwo("77", "88")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	morehops, nexthop, err := t2.GetNextHop(MakeID("77"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, true)
	cc.Assert(nexthop, Equals, t1.node)
}

// getnexthop: morehops = true when appropriate
func (s *TopologySuite) TestTA_GetNextHop2_3_Test(cc *C) {
	t1, t2, err := MakeTwo("77", "88")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	morehops, nexthop, err := t1.GetNextHop(MakeID("88"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, true)
	cc.Assert(nexthop, Equals, t2.node)
}

// getnexthop: morehops = true when appropriate
func (s *TopologySuite) TestTA_GetNextHop2_4_Test(cc *C) {
	t1, t2, err := MakeTwo("77", "88")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	morehops, nexthop, err := t2.GetNextHop(MakeID("88"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, false)
	cc.Assert(nexthop, Equals, t2.node)
}

// getnexthop: morehops = true when appropriate
func (s *TopologySuite) TestTA_GetNextHop2_5_Test(cc *C) {
	t1, t2, err := MakeTwo("77", "88")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	morehops, nexthop, err := t1.GetNextHop(MakeID("AEFC"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, false)
	cc.Assert(nexthop, Equals, t1.node)
}

// getnexthop: morehops = true when appropriate
func (s *TopologySuite) TestTA_GetNextHop2_6_Test(cc *C) {
	t1, t2, err := MakeTwo("77", "88")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	morehops, nexthop, err := t2.GetNextHop(MakeID("AEFC"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, true)
	cc.Assert(nexthop, Equals, t1.node)
}

// getnexthop: nexthop is correct when morehops = true
func (s *TopologySuite) TestTA_GetNextHop4_1_Test(cc *C) {
	ts, err := MakeTapestries(true, "2277", "2288", "2299", "22AA")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	morehops, nexthop, err := ts[0].GetNextHop(MakeID("7080"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, true)
	cc.Assert(nexthop, Equals, ts[1].node)
}

// getnexthop: nexthop is correct when morehops = true
func (s *TopologySuite) TestTA_GetNextHop4_2_Test(cc *C) {
	ts, err := MakeTapestries(true, "2277", "2288", "2299", "22AA")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	morehops, nexthop, err := ts[1].GetNextHop(MakeID("7080"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, false)
	cc.Assert(nexthop, Equals, ts[1].node)
}

// getnexthop: nexthop is correct when morehops = true
func (s *TopologySuite) TestTA_GetNextHop4_3_Test(cc *C) {
	ts, err := MakeTapestries(true, "2277", "2288", "2299", "22AA")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	morehops, nexthop, err := ts[2].GetNextHop(MakeID("7080"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, true)
	cc.Assert(nexthop, Equals, ts[1].node)
}

// getnexthop: nexthop is correct when morehops = true
func (s *TopologySuite) TestTA_GetNextHop4_4_Test(cc *C) {
	ts, err := MakeTapestries(true, "2277", "2288", "2299", "22AA")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	morehops, nexthop, err := ts[3].GetNextHop(MakeID("7080"))

	cc.Assert(err, IsNil)
	cc.Assert(morehops, Equals, true)
	cc.Assert(nexthop, Equals, ts[1].node)
}

// Leave: after leaving, node no longer exists in any RT
func (s *TopologySuite) TestTA_Leave4_RoutingTable_Test(cc *C) {
	ts, err := MakeTapestries(true, "1111", "2222", "3333", "4444")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	time.Sleep(500 * time.Millisecond)

	cc.Assert(hasRoutingTableEntry(ts[0], ts[3]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[1], ts[3]), Equals, true)
	cc.Assert(hasRoutingTableEntry(ts[2], ts[3]), Equals, true)
	cc.Assert(hasBackpointer(ts[3], ts[0]), Equals, true)
	cc.Assert(hasBackpointer(ts[3], ts[1]), Equals, true)
	cc.Assert(hasBackpointer(ts[3], ts[2]), Equals, true)

	ts[3].Leave()

	time.Sleep(100 * time.Millisecond)

	cc.Assert(hasRoutingTableEntry(ts[0], ts[3]), Equals, false)
	cc.Assert(hasRoutingTableEntry(ts[1], ts[3]), Equals, false)
	cc.Assert(hasRoutingTableEntry(ts[2], ts[3]), Equals, false)
}

// Leave: provide a replacement if one in backpointers
func (s *TopologySuite) TestTA_Leave5_Replacement_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)
	t3, err := start(MakeID("2222"), 0, t2.node.Address)
	cc.Assert(err, IsNil)
	t4, err := start(MakeID("2333"), 0, t2.node.Address)
	cc.Assert(err, IsNil)
	t5, err := start(MakeID("2444"), 0, t2.node.Address)
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2, t3, t4, t5)

	time.Sleep(500 * time.Millisecond)

	cc.Assert(hasRoutingTableEntry(t1, t5), Equals, false)
	cc.Assert(hasRoutingTableEntry(t2, t5), Equals, true)
	cc.Assert(hasRoutingTableEntry(t3, t5), Equals, true)
	cc.Assert(hasRoutingTableEntry(t4, t5), Equals, true)
	cc.Assert(hasBackpointer(t5, t1), Equals, false)
	cc.Assert(hasBackpointer(t5, t2), Equals, true)
	cc.Assert(hasBackpointer(t5, t3), Equals, true)
	cc.Assert(hasBackpointer(t5, t4), Equals, true)

	t2.Leave()
	t3.Leave()
	t4.Leave()

	time.Sleep(100 * time.Millisecond)

	cc.Assert(hasRoutingTableEntry(t1, t5), Equals, true)
	cc.Assert(hasBackpointer(t5, t1), Equals, true)
}

// Find root starting at a bad node
func (s *TopologySuite) TestTA_FindRoot1_BadNode_Test(cc *C) {
	t1, err := MakeOne("10")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	badnode := RemoteNode{MakeID("20"), "asdf"}
	t1.table.Add(badnode)

	root, _, err := t1.findRoot(t1.node, badnode.Id)

	cc.Assert(err, IsNil)
	cc.Assert(root, Equals, t1.node)
	cc.Assert(hasRoutingTableNode(t1, badnode), Equals, false)
}

// Find root with one better bad node returns to self
func (s *TopologySuite) TestTA_FindRoot2_BadNode_Test(cc *C) {
	t1, t2, err := MakeTwo("10", "20")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	time.Sleep(100 * time.Millisecond)

	root, _,  err := t1.findRoot(t1.node, t2.node.Id)

	cc.Assert(err, IsNil)
	cc.Assert(root, Equals, t2.node)

	KillTapestries(t2)

	time.Sleep(100 * time.Millisecond)

	root, _, err = t1.findRoot(t1.node, t2.node.Id)

	cc.Assert(err, IsNil)
	cc.Assert(root, Equals, t1.node)

	KillTapestries(t1)
}

// Find root with one bad node removes bad node
func (s *TopologySuite) TestTA_FindRoot1_BadNode_2_Test(cc *C) {
	t1, err := MakeOne("10")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1)

	badnode := RemoteNode{MakeID("20"), "asdf"}
	t1.table.Add(badnode)

	root, _, err := t1.findRoot(t1.node, badnode.Id)

	cc.Assert(err, IsNil)
	cc.Assert(root, Equals, t1.node)
	cc.Assert(hasRoutingTableNode(t1, badnode), Equals, false)
}
