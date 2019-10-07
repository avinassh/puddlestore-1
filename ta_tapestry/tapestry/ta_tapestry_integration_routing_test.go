package tapestry

import (
	"time"

	. "gopkg.in/check.v1"
)

type RoutingIntegrationSuite struct{}

var _ = Suite(&RoutingIntegrationSuite{})

/*
 * Unit tests for Tapestry topology: join / leave / addbackpointer, etc.
 */

func (o *RoutingIntegrationSuite) TearDownTest(cc *C) {

	time.Sleep(1 * time.Second)
	closeAllConnections()
}

// Helper method - create several pseudo-random tapestry nodes and route to various IDs
func runFindRootTest(seed int64, count int, numtests int, cc *C) {
	ts, err := MakeRandomTapestries(seed, count)
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	time.Sleep(1 * time.Second)

	for _, t1 := range ts {
		for _, t2 := range ts {
			if numtests == 0 {
				break
			}
			root, _, err := t1.findRoot(t1.node, t2.node.Id)

			cc.Check(err, IsNil)
			cc.Check(root, Equals, t2.node)
			numtests--
		}
	}
}

type testresult struct {
	expected RemoteNode
	actual   RemoteNode
	err      error
}

// Helper method - create several pseudo-random tapestry nodes and route to various IDs
func runFindRootTestConcurrent(seed int64, count int, cc *C) {
	ts, err := MakeRandomTapestries(seed, count)
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	results := make(chan testresult)
	count = 0

	for _, t1 := range ts {
		for _, t2 := range ts {
			go func(a, b *Node) {
				var result testresult
				result.expected = b.node
				result.actual, _, result.err = a.findRoot(a.node, b.node.Id)
				results <- result
			}(t1, t2)
			count++
		}
	}

	for count > 0 {
		result := <-results
		count--

		cc.Check(result.err, IsNil)
		cc.Check(result.actual, Equals, result.expected)
	}

}

// 1 node tapestry start then kill
func (s *RoutingIntegrationSuite) TestTA_TapestryStart1_Test(cc *C) {
	ts, err := MakeTapestries(true, "1")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()
}

// 2 node tapestry start then kill
func (s *RoutingIntegrationSuite) TestTA_TapestryStart2_Test(cc *C) {
	ts, err := MakeTapestries(true, "1", "2")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()
}

// 5 node tapestry start then kill
func (s *RoutingIntegrationSuite) TestTA_TapestryStart5_Test(cc *C) {
	ts, err := MakeTapestries(true, "1", "2", "3", "4", "5")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()
}

// 5 node tapestry start then kill
func (s *RoutingIntegrationSuite) TestTA_TapestryStart5_2_Test(cc *C) {
	ts, err := MakeTapestries(true, "00101", "1100", "1110", "01010", "00110")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()
}

// 1-node find root test
func (s *RoutingIntegrationSuite) TestTA_FindRoot1_1_Test(cc *C) {
	tapestry, err := MakeOne("3031FAFD")
	cc.Assert(err, IsNil)
	defer KillTapestries(tapestry)

	node := tapestry.node

	// Check the local node's ID
	root, _, err := tapestry.findRoot(node, node.Id)
	cc.Check(err, IsNil)
	cc.Check(root, Equals, node)
}

// 1-node find root test
func (s *RoutingIntegrationSuite) TestTA_FindRoot1_2_Test(cc *C) {
	tapestry, err := MakeOne("772038A")
	cc.Assert(err, IsNil)
	defer KillTapestries(tapestry)

	node := tapestry.node

	// Check a random ID, differing at digit 0
	root, _, err := tapestry.findRoot(node, MakeID("582AFD"))
	cc.Check(err, IsNil)
	cc.Check(root, Equals, node)
}

// 1-node find root test
func (s *RoutingIntegrationSuite) TestTA_FindRoot1_3_Test(cc *C) {
	tapestry, err := MakeOne("99ED9E3")
	cc.Assert(err, IsNil)
	defer KillTapestries(tapestry)

	node := tapestry.node

	// Check one more random ID, differing at digit 5
	root, _, err := tapestry.findRoot(node, MakeID("99ED8E3"))
	cc.Check(err, IsNil)
	cc.Check(root, Equals, node)
}

// 2-node find root tests
func (s *RoutingIntegrationSuite) TestTA_FindRoot2_1_Test(cc *C) {
	t1, t2, err := MakeTwo("11100", "115000")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	// Check whether t1 finds t2's ID
	root, _, err := t1.findRoot(t1.node, t2.node.Id)
	cc.Check(err, IsNil)
	cc.Check(root, Equals, t2.node)
}

// 2-node find root tests
func (s *RoutingIntegrationSuite) TestTA_FindRoot2_2_Test(cc *C) {
	t1, t2, err := MakeTwo("11100", "115000")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	// Check whether t2 finds t1's ID
	root, _, err := t2.findRoot(t2.node, t1.node.Id)
	cc.Check(err, IsNil)
	cc.Check(root, Equals, t1.node)
}

// 2-node find root tests
func (s *RoutingIntegrationSuite) TestTA_FindRoot2_3_Test(cc *C) {
	t1, t2, err := MakeTwo("11100", "115000")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	// Check whether a random ID finds t2, starting from t1
	root, _, err := t1.findRoot(t1.node, MakeID("113000"))
	cc.Check(err, IsNil)
	cc.Check(root, Equals, t2.node)
}

// 2-node find root tests
func (s *RoutingIntegrationSuite) TestTA_FindRoot2_4_Test(cc *C) {
	t1, t2, err := MakeTwo("11100", "115000")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	// Check whether a random ID finds t2, starting from t2
	root, _, err := t2.findRoot(t2.node, MakeID("113000"))
	cc.Check(err, IsNil)
	cc.Check(root, Equals, t2.node)
}

// 2-node find root tests
func (s *RoutingIntegrationSuite) TestTA_FindRoot2_5_Test(cc *C) {
	t1, t2, err := MakeTwo("11100", "115000")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	// Check whether a random ID finds t2, starting from t1
	root, _, err := t1.findRoot(t1.node, MakeID("11AAAA"))
	cc.Check(err, IsNil)
	cc.Check(root, Equals, t1.node)
}

// 2-node find root tests
func (s *RoutingIntegrationSuite) TestTA_FindRoot2_6_Test(cc *C) {
	t1, t2, err := MakeTwo("11100", "115000")
	cc.Assert(err, IsNil)
	defer KillTapestries(t1, t2)

	// Check whether a random ID finds t2, starting from t2
	root, _, err := t2.findRoot(t2.node, MakeID("11AAAA"))
	cc.Check(err, IsNil)
	cc.Check(root, Equals, t1.node)
}

// Find root on an 8-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot8_1_Test(cc *C) {
	ts, err := MakeTapestries(true, "11100", "115000", "AAFF88", "ADFF88", "AFFF99", "A13293", "119900", "122000")
	cc.Assert(err, IsNil)
	defer KillTapestries(ts...)

	TapestryPause()

	// Check whether each node can find each other node
	for _, t1 := range ts {
		for _, t2 := range ts {
			root, _, err := t1.findRoot(t1.node, t2.node.Id)

			cc.Check(err, IsNil)
			cc.Check(root, Equals, t2.node)
		}
	}
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_1_Test(cc *C) {
	runFindRootTest(1, 16, 10, cc)
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_2_Test(cc *C) {
	runFindRootTest(2, 16, 10, cc)
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_3_Test(cc *C) {
	runFindRootTest(3, 16, 10, cc)
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_4_Test(cc *C) {
	runFindRootTest(4, 16, 10, cc)
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_5_Test(cc *C) {
	runFindRootTest(5, 16, 10, cc)
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_Concurrent_1_Test(cc *C) {
	runFindRootTestConcurrent(1, 16, cc)
}

// FindRoot on a 16-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot16_Concurrent_2_Test(cc *C) {
	runFindRootTestConcurrent(2, 16, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_1_Test(cc *C) {
	runFindRootTest(1, 32, 10, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_2_Test(cc *C) {
	runFindRootTest(2, 32, 10, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_3_Test(cc *C) {
	runFindRootTest(3, 32, 10, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_4_Test(cc *C) {
	runFindRootTest(4, 32, 10, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_5_Test(cc *C) {
	runFindRootTest(5, 32, 10, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_Concurrent_1_Test(cc *C) {
	runFindRootTestConcurrent(1, 32, cc)
}

// FindRoot on a 32-node cluster
func (s *RoutingIntegrationSuite) TestTA_FindRoot32_Concurrent_2_Test(cc *C) {
	runFindRootTestConcurrent(2, 32, cc)
}
