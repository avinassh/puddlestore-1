package tapestry

import (
	. "gopkg.in/check.v1"
)

type RoutingTableSuite struct{}

var _ = Suite(&RoutingTableSuite{})

// Check can't add local node
func (s *RoutingTableSuite) TestTA_Add1_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	added, _ := table.Add(node)
	cc.Assert(added, Equals, false) // should be false because already in table
}

// Check can add some other node
func (s *RoutingTableSuite) TestTA_Add2_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	added, _ := table.Add(RemoteNode{MakeID("2"), "asdf"})
	cc.Assert(added, Equals, true)
}

// Check can't add node twice
func (s *RoutingTableSuite) TestTA_Add3_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	added, _ := table.Add(RemoteNode{MakeID("2"), "asdf"})
	added, _ = table.Add(RemoteNode{MakeID("2"), "asdf"})
	cc.Assert(added, Equals, false)
}

// Check can't add node twice and prev is correct
func (s *RoutingTableSuite) TestTA_Add4_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	_, prev := table.Add(RemoteNode{MakeID("2"), "asdf"})
	_, prev = table.Add(RemoteNode{MakeID("2"), "asdf"})
	cc.Assert(prev, IsNil)
}

// Check add SLOTSIZE many nodes
func (s *RoutingTableSuite) TestTA_Add5_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	added, _ := table.Add(RemoteNode{MakeID("20"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("21"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("22"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("23"), "asdf"})
	cc.Assert(added, Equals, false)
}

// Check furthest node is correctly removed from table
func (s *RoutingTableSuite) TestTA_FurthestNodeRemovedFromSlot_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	added, _ := table.Add(RemoteNode{MakeID("23"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("21"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("22"), "asdf"})
	cc.Assert(added, Equals, true)
	added, prev := table.Add(RemoteNode{MakeID("20"), "asdf"})
	cc.Assert(added, Equals, true)
	cc.Assert(*prev, Equals, RemoteNode{MakeID("23"), "asdf"})
}

// Check furthest node is correctly removed from table (2)
func (s *RoutingTableSuite) TestTA_FurthestNodeRemovedFromSlot2_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	added, _ := table.Add(RemoteNode{MakeID("21"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("23"), "asdf"})
	cc.Assert(added, Equals, true)
	added, _ = table.Add(RemoteNode{MakeID("22"), "asdf"})
	cc.Assert(added, Equals, true)
	added, prev := table.Add(RemoteNode{MakeID("20"), "asdf"})
	cc.Assert(added, Equals, true)
	cc.Assert(*prev, Equals, RemoteNode{MakeID("23"), "asdf"})
}

// Check added node resides in correct slot
func (s *RoutingTableSuite) TestTA_Add6_Test(cc *C) {
	node := RemoteNode{MakeID("1"), "testroutingtable"}
	table := NewRoutingTable(node)

	table.Add(RemoteNode{MakeID("23"), "asdf"})
	cc.Assert((*table.rows[0][2])[0], Equals, RemoteNode{MakeID("23"), "asdf"})
}

// Check added node resides in correct slot (2)
func (s *RoutingTableSuite) TestTA_Add7_Test(cc *C) {
	node := RemoteNode{MakeID("12340"), "testroutingtable"}
	table := NewRoutingTable(node)

	n := RemoteNode{MakeID("12348"), "asdf"}
	table.Add(n)
	cc.Assert((*table.rows[4][8])[0], Equals, n)
}

// Check routing table has correct layout (local node in slots on every level,
// second node in correct slot on 4th level
// note: undefined behavior for second node on slots 0,1,2,3
func (s *RoutingTableSuite) TestTA_Add8_Test(cc *C) {
	nid := MakeID("AB92302")
	node := RemoteNode{nid, "testroutingtable"}
	table := NewRoutingTable(node)

	nid2 := MakeID("AB92990")
	n := RemoteNode{nid2, "asdf"}
	table.Add(n)

	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			if Digit(j) == nid[i] {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert((*table.rows[i][j])[0], Equals, node)
			} else if Digit(j) == nid2[i] && i <= 4 {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert((*table.rows[i][j])[0], Equals, n)
			} else {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert(len(*table.rows[i][j]), Equals, 0)
			}
		}
	}
}

// Check can't remove local node
func (s *RoutingTableSuite) TestTA_Remove1_Test(cc *C) {
	node := RemoteNode{MakeID("12340"), "testroutingtable"}
	table := NewRoutingTable(node)
	removed := table.Remove(node)
	cc.Assert(removed, Equals, false)
}

// Check local node still exists after attempted removal
func (s *RoutingTableSuite) TestTA_Remove2_Test(cc *C) {
	nid := MakeID("AB92302")
	node := RemoteNode{nid, "testroutingtable"}
	table := NewRoutingTable(node)
	table.Remove(node)

	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			if Digit(j) == nid[i] {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert(len(*table.rows[i][j]), Equals, 1)
				cc.Assert((*table.rows[i][j])[0], Equals, node)
			} else {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert(len(*table.rows[i][j]), Equals, 0)
			}
		}
	}
}

// Check remove does actual remove
func (s *RoutingTableSuite) TestTA_Remove3_Test(cc *C) {
	node := RemoteNode{MakeID("12340"), "testroutingtable"}
	table := NewRoutingTable(node)
	n := RemoteNode{MakeID("12348"), "asdf"}
	added, _ := table.Add(n)
	cc.Assert(added, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 1)
	removed := table.Remove(n)
	cc.Assert(removed, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 0)
}

// Check entire table after remove
func (s *RoutingTableSuite) TestTA_Remove4_Test(cc *C) {
	nid := MakeID("AB92302")
	node := RemoteNode{nid, "testroutingtable"}
	table := NewRoutingTable(node)

	nid2 := MakeID("AB92990")
	n := RemoteNode{nid2, "asdf"}
	table.Add(n)
	table.Remove(n)

	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			if Digit(j) == nid[i] {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert(len(*table.rows[i][j]), Equals, 1)
				cc.Assert((*table.rows[i][j])[0], Equals, node)
			} else {
				cc.Assert(table.rows[i][j], NotNil)
				cc.Assert(len(*table.rows[i][j]), Equals, 0)
			}
		}
	}
}

// Check remove of node that isn't in table
func (s *RoutingTableSuite) TestTA_Remove5_Test(cc *C) {
	node := RemoteNode{MakeID("12340"), "testroutingtable"}
	table := NewRoutingTable(node)
	nid2 := MakeID("AB92990")
	n := RemoteNode{nid2, "asdf"}
	removed := table.Remove(n)
	cc.Assert(removed, Equals, false)
}

// Check remove node several times
func (s *RoutingTableSuite) TestTA_Remove6_Test(cc *C) {
	node := RemoteNode{MakeID("12340"), "testroutingtable"}
	table := NewRoutingTable(node)
	n := RemoteNode{MakeID("12348"), "asdf"}
	added, _ := table.Add(n)
	cc.Assert(added, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 1)
	removed := table.Remove(n)
	cc.Assert(removed, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 0)
	removed = table.Remove(n)
	cc.Assert(removed, Equals, false)
	cc.Assert(len(*table.rows[4][8]), Equals, 0)
}

// Check re-add node after removal
func (s *RoutingTableSuite) TestTA_AddRemove1_Test(cc *C) {
	node := RemoteNode{MakeID("12340"), "testroutingtable"}
	table := NewRoutingTable(node)
	n := RemoteNode{MakeID("12348"), "asdf"}
	added, _ := table.Add(n)
	cc.Assert(added, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 1)
	removed := table.Remove(n)
	cc.Assert(removed, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 0)
	added, _ = table.Add(n)
	cc.Assert(added, Equals, true)
	cc.Assert(len(*table.rows[4][8]), Equals, 1)
}

func (s *RoutingTableSuite) TestTA_GetNextHop0_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	next := table.GetNextHop(myid)
	cc.Assert(next, Equals, mynode)
}

func (s *RoutingTableSuite) TestTA_GetNextHop1_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("12440"), "asdf"}
	table.Add(nodeb)

	next := table.GetNextHop(MakeID("12340"))
	cc.Assert(next, Equals, mynode)
}

func (s *RoutingTableSuite) TestTA_GetNextHop2_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("12440"), "asdf"}
	table.Add(nodeb)

	next := table.GetNextHop(MakeID("12440"))
	cc.Assert(next, Equals, nodeb)
}

func (s *RoutingTableSuite) TestTA_GetNextHop3_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("12440"), "asdf"}
	table.Add(nodeb)

	next := table.GetNextHop(MakeID("19440"))
	cc.Assert(next, Equals, nodeb)
}

func (s *RoutingTableSuite) TestTA_GetNextHop4_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("12440"), "asdf"}
	table.Add(nodeb)

	next := table.GetNextHop(MakeID("19340"))
	cc.Assert(next, Equals, mynode)
}

func (s *RoutingTableSuite) TestTA_GetNextHop5_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("12440"), "asdf"}
	table.Add(nodeb)

	next := table.GetNextHop(MakeID("92702"))
	cc.Assert(next, Equals, mynode)
}

func (s *RoutingTableSuite) TestTA_GetNextHop6_Test(cc *C) {
	myid := MakeID("12340")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("12440"), "asdf"}
	table.Add(nodeb)

	next := table.GetNextHop(MakeID("92402"))
	cc.Assert(next, Equals, nodeb)
}

func (s *RoutingTableSuite) TestTA_GetNextHop7_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("1"))
	cc.Assert(next, Equals, mynode)
}

func (s *RoutingTableSuite) TestTA_GetNextHop8_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("11"))
	cc.Assert(next, Equals, nodec)
}

func (s *RoutingTableSuite) TestTA_GetNextHop9_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("0"))
	cc.Assert(next, Equals, mynode)
}

func (s *RoutingTableSuite) TestTA_GetNextHop10_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("6"))
	cc.Assert(next, Equals, nodeb)
}

func (s *RoutingTableSuite) TestTA_GetNextHop11_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("9"))
	cc.Assert(next, Equals, noded)
}

func (s *RoutingTableSuite) TestTA_GetNextHop12_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("4"))
	cc.Assert(next, Equals, nodeb)
}

func (s *RoutingTableSuite) TestTA_GetNextHop13_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("8"))
	cc.Assert(next, Equals, noded)
}

func (s *RoutingTableSuite) TestTA_GetNextHop14_Test(cc *C) {
	myid := MakeID("1")
	mynode := RemoteNode{myid, "testroutingtable"}
	table := NewRoutingTable(mynode)

	nodeb := RemoteNode{MakeID("6"), "asdf"}
	table.Add(nodeb)
	nodec := RemoteNode{MakeID("11"), "asdf"}
	table.Add(nodec)
	noded := RemoteNode{MakeID("9"), "asdf"}
	table.Add(noded)

	next := table.GetNextHop(MakeID("E"))
	cc.Assert(next, Equals, mynode)
}
