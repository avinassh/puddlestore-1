package tapestry

import (
	"time"

	. "gopkg.in/check.v1"
)

type ObjectsAPISuite struct{}

var _ = Suite(&ObjectsAPISuite{})

func (o *ObjectsAPISuite) TearDownTest(cc *C) {
	time.Sleep(1 * time.Second)
	closeAllConnections()
}

// Test Transfer adds the transferred objects to the replica map
func (s *ObjectsAPISuite) TestTA_Transfer1_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, "")
	cc.Assert(err, IsNil)

	transferring := make(map[string][]RemoteNode)
	transferring["test"] = append(transferring["test"], t2.node)

	t1.Transfer(t2.node, transferring)

	_, exists := t1.locationsByKey.data["test"]

	cc.Assert(exists, Equals, true)

	_, exists = t1.locationsByKey.data["test"][t2.node]

	cc.Assert(exists, Equals, true)

	t1.Kill()
	t2.Kill()

}

// Test transfer adds the from node as a route
func (s *ObjectsAPISuite) TestTA_Transfer2_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, "")
	cc.Assert(err, IsNil)

	transferring := make(map[string][]RemoteNode)
	transferring["test"] = append(transferring["test"], t2.node)

	t1.Transfer(t2.node, transferring)

	time.Sleep(100 * time.Millisecond)

	cc.Assert(hasRoutingTableEntry(t1, t2), Equals, true)

	t1.Kill()
	t2.Kill()

}

// Test fetch returns true if we are the root node,
// regardless of whether we are storing
func (s *ObjectsAPISuite) TestTA_Fetch1_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	isRoot, replicas, err := t1.Fetch("test")
	cc.Check(isRoot, Equals, true)
	cc.Check(len(replicas), Equals, 0)
	cc.Check(err, IsNil)

	t1.Kill()
}

// Test fetch returns true if we are the root node and storing
func (s *ObjectsAPISuite) TestTA_Fetch2_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t1.Register("test", t1.node)
	isRoot, replicas, err := t1.Fetch("test")
	cc.Check(isRoot, Equals, true)
	cc.Check(err, IsNil)
	cc.Assert(len(replicas), Equals, 1)
	cc.Check(replicas[0], Equals, t1.node)

	t1.Kill()
}

// Test fetch returns false if we are not the root node
func (s *ObjectsAPISuite) TestTA_Fetch3_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	isRoot, replicas, err := t2.Fetch("test")

	cc.Check(isRoot, Equals, false)
	cc.Check(err, IsNil)
	cc.Assert(len(replicas), Equals, 0)

	t1.Kill()
	t2.Kill()
}

// Test fetch returns false if we are not the root node (2)
func (s *ObjectsAPISuite) TestTA_Fetch4_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Register("test", t1.node)

	isRoot, replicas, err := t2.Fetch("test")

	cc.Check(isRoot, Equals, false)
	cc.Check(err, IsNil)
	cc.Assert(len(replicas), Equals, 0)

	t1.Kill()
	t2.Kill()
}

// Test fetch returns replicas if we have something registered
func (s *ObjectsAPISuite) TestTA_Fetch5_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Register("test", t2.node)

	isRoot, replicas, err := t1.Fetch("test")

	cc.Check(isRoot, Equals, true)
	cc.Check(err, IsNil)
	cc.Assert(len(replicas), Equals, 1)
	cc.Check(replicas[0], Equals, t2.node)

	t1.Kill()
	t2.Kill()
}

// Test fetch returns multiple replicas
func (s *ObjectsAPISuite) TestTA_Fetch6_Test(cc *C) {
	t1, err := start(MakeID("1"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Register("test", t2.node)
	t1.Register("test", t1.node)

	isRoot, replicas, err := t1.Fetch("test")

	cc.Check(isRoot, Equals, true)
	cc.Check(err, IsNil)
	cc.Assert(len(replicas), Equals, 2)
	if replicas[0] == t1.node {
		cc.Check(replicas[0], Equals, t1.node)
		cc.Check(replicas[1], Equals, t2.node)
	} else {
		cc.Check(replicas[0], Equals, t2.node)
		cc.Check(replicas[1], Equals, t1.node)
	}

	t1.Kill()
	t2.Kill()
}

// Publish: registers object on correct root for local root
func (s *ObjectsAPISuite) TestTa_Publish1_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	_, err = t1.Publish("test")

	cc.Assert(err, IsNil)
	cc.Assert(len(t1.locationsByKey.Get("test")), Equals, 1)
	cc.Assert(len(t2.locationsByKey.Get("test")), Equals, 0)
	cc.Assert(t1.locationsByKey.Get("test")[0], Equals, t1.node)

	t1.Kill()
	t2.Kill()
}

// Publish: registers object on correct root for remote root
// FAILING THIS TEST IS FINE BECAUSE WE ADD THE LOCATION OF THE KEY TO THE MAP
// IN T2 -- WE ACTUALLY WANT t2.locationsByKey.Get("test") == 1
func (s *ObjectsAPISuite) TestTa_Publish2_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	_, err = t2.Publish("test")

	cc.Assert(err, IsNil)
	cc.Assert(len(t1.locationsByKey.Get("test")), Equals, 1)
	cc.Assert(len(t2.locationsByKey.Get("test")), Equals, 1)
	cc.Assert(t1.locationsByKey.Get("test")[0], Equals, t2.node)

	t1.Kill()
	t2.Kill()
}

// Publish: from multiple nodes, both go to correct root
func (s *ObjectsAPISuite) TestTa_Publish3_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	_, err = t1.Publish("test")
	cc.Assert(err, IsNil)
	_, err = t2.Publish("test")
	cc.Assert(err, IsNil)
	cc.Assert(len(t1.locationsByKey.Get("test")), Equals, 2)
	//cc.Assert(len(t2.locationsByKey.Get("test")), Equals, 0)
	// should equal 1 because when we publish from t2 we put it the location
	// in its locationMap
	cc.Assert(len(t2.locationsByKey.Get("test")), Equals, 1)

	t1.Kill()
	t2.Kill()
}

// Lookup from surrogate successful
func (s *ObjectsAPISuite) TestTA_Lookup1_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Publish("test")

	nodes, err := t1.Lookup("test")
	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 1)
	cc.Assert(nodes[0], Equals, t1.node)

	t1.Kill()
	t2.Kill()
}

// Lookup from non-surrogate successful
func (s *ObjectsAPISuite) TestTA_Lookup2_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Publish("test")

	nodes, err := t2.Lookup("test")
	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 1)
	cc.Assert(nodes[0], Equals, t1.node)

	t1.Kill()
	t2.Kill()
}

// Lookup returns multiple nodes
func (s *ObjectsAPISuite) TestTA_Lookup3_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Publish("test")
	t2.Publish("test")

	nodes, err := t1.Lookup("test")
	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 2)

	t1.Kill()
	t2.Kill()
}

// Lookup returns multiple nodes
// THIS TEST SHOULD FAIL BECAUSE WE WILL SHORTCUT AND SIMPLY ROUTE TO OURSELF
func (s *ObjectsAPISuite) TestTA_Lookup4_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	t1.Publish("test")
	t2.Publish("test")

	nodes, err := t2.Lookup("test")
	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 2)

	t1.Kill()
	t2.Kill()
}

// Lookup: lookup returns nothing if not being publishes
func (s *ObjectsAPISuite) TestTA_Lookup5_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	nodes, err := t1.Lookup("test")

	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 0)

	t1.Kill()
	t2.Kill()
}

// Lookup: lookup returns nothing if not being publishes
func (s *ObjectsAPISuite) TestTA_Lookup6_Test(cc *C) {
	t1, err := start(MakeID("1111"), 0, "")
	cc.Assert(err, IsNil)
	t2, err := start(MakeID("2111"), 0, t1.node.Address)
	cc.Assert(err, IsNil)

	nodes, err := t2.Lookup("test")

	cc.Assert(err, IsNil)
	cc.Assert(len(nodes), Equals, 0)

	t1.Kill()
	t2.Kill()
}
