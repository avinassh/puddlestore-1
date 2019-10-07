package tapestry

import (
	"fmt"
	"time"

	. "gopkg.in/check.v1"
)

type ObjectsIntegrationSuite struct{}

var _ = Suite(&ObjectsIntegrationSuite{})

func (o *ObjectsIntegrationSuite) TearDownTest(cc *C) {
	time.Sleep(1 * time.Second)
	closeAllConnections()
}

func doPublish(data []PublishSpec, cc *C) {
	for _, d := range data {
		err := d.store.Store(d.key, []byte{})
		cc.Assert(err, IsNil)
		time.Sleep(10 * time.Millisecond)
	}
}

func doLookup(data []PublishSpec, cc *C) {
	for _, d := range data {
		nodes, err := d.lookup.Lookup(d.key)
		cc.Assert(err, IsNil)
		cc.Assert(hasnode(nodes, d.store.node), Equals, true)
		time.Sleep(10 * time.Millisecond)
	}
}

func doPublishLookup(seed, count, numtests int, cc *C) {
	aseed := int64(1)

	ts, err := MakeRandomTapestries(aseed, count)

	cc.Assert(err, IsNil)

	time.Sleep(1 * time.Second)

	data := GenerateData(aseed, numtests, ts)

	doPublish(data, cc)

	time.Sleep(1 * time.Second)

	doLookup(data, cc)
}

func doPublishAddLookup(seed, count, numtests int, cc *C) {
	aseed := int64(1)

	ts, err := MakeRandomTapestries(aseed, count)

	cc.Assert(err, IsNil)

	time.Sleep(1 * time.Second)

	data := GenerateData(aseed, numtests, ts)

	doPublish(data, cc)

	time.Sleep(1 * time.Second)

	ts, err = MakeMoreRandomTapestries(aseed, count, ts)

	cc.Assert(err, IsNil)

	time.Sleep(1 * time.Second)

	doLookup(data, cc)
}

func doPublishAddKillLookup(seed, count, numtests int, cc *C) {
	aseed := int64(1)

	ts, err := MakeRandomTapestries(aseed, count)

	cc.Assert(err, IsNil)

	time.Sleep(1 * time.Second)

	data := GenerateData(aseed, numtests, ts)

	ts, err = MakeMoreRandomTapestries(aseed, count, ts)

	doPublish(data, cc)

	time.Sleep(1 * time.Second)

	for i := count; i < len(ts); i++ {
		KillTapestries(ts[i])
	}

	time.Sleep(REPUBLISH + 1*time.Second)

	doLookup(data, cc)
}

// Integration tests 1: start 5 nodes, publish objects, lookup objects
func (s *ObjectsIntegrationSuite) TestTA_IntegrationTest1_Test(cc *C) {
	ids := []string{
		"1027310",
		"0273729",
		"AE1027F",
		"293FD29",
		"77F8E8C",
	}
	ts := make([]*Node, 0, len(ids))
	t, err := start(MakeID(ids[0]), 0, "")
	cc.Assert(err, IsNil)
	ts = append(ts, t)
	for i := 1; i < len(ids); i++ {
		t, err = start(MakeID(ids[i]), 0, ts[0].node.Address)
		cc.Assert(err, IsNil)
		ts = append(ts, t)
	}

	time.Sleep(300 * time.Millisecond)

	for i := 0; i < len(ids); i++ {
		ts[i].Store(fmt.Sprintf("obj-%v", i), []byte{})
	}

	for i := 0; i < len(ids); i++ {
		ts[i].Lookup(fmt.Sprintf("obj-%v", i+2))
	}

}

// 5 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup5_1_Test(cc *C) {
	doPublishLookup(1, 5, 10, cc)
}

// 5 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup5_2_Test(cc *C) {
	doPublishLookup(2, 5, 10, cc)
}

// 5 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup5_3_Test(cc *C) {
	doPublishLookup(3, 5, 10, cc)
}

// 5 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup5_4_Test(cc *C) {
	doPublishLookup(4, 5, 10, cc)
}

// 5 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup5_5_Test(cc *C) {
	doPublishLookup(5, 5, 10, cc)
}

// 10 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup10_1_Test(cc *C) {
	doPublishLookup(1, 10, 10, cc)
}

// 10 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup10_2_Test(cc *C) {
	doPublishLookup(2, 10, 10, cc)
}

// 10 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup10_3_Test(cc *C) {
	doPublishLookup(3, 10, 10, cc)
}

// 10 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup10_4_Test(cc *C) {
	doPublishLookup(4, 10, 10, cc)
}

// 10 node publish-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishLookup10_5_Test(cc *C) {
	doPublishLookup(5, 10, 10, cc)
}

// 5 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup5_1_Test(cc *C) {
	doPublishAddLookup(1, 5, 10, cc)
}

// 5 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup5_2_Test(cc *C) {
	doPublishAddLookup(2, 5, 10, cc)
}

// 5 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup5_3_Test(cc *C) {
	doPublishAddLookup(3, 5, 10, cc)
}

// 5 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup5_4_Test(cc *C) {
	doPublishAddLookup(4, 5, 10, cc)
}

// 5 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup5_5_Test(cc *C) {
	doPublishAddLookup(5, 5, 10, cc)
}

// 10 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup10_1_Test(cc *C) {
	doPublishAddLookup(1, 10, 10, cc)
}

// 10 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup10_2_Test(cc *C) {
	doPublishAddLookup(2, 10, 10, cc)
}

// 10 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup10_3_Test(cc *C) {
	doPublishAddLookup(3, 10, 10, cc)
}

// 10 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup10_4_Test(cc *C) {
	doPublishAddLookup(4, 10, 10, cc)
}

// 10 node publish-add-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddLookup10_5_Test(cc *C) {
	doPublishAddLookup(5, 10, 10, cc)
}

// 5 node publish-add-kill-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddKillLookup5_1_Test(cc *C) {
	doPublishAddKillLookup(1, 5, 10, cc)
}

// 10 node publish-add-kill-lookup test
func (s *ObjectsIntegrationSuite) TestTA_PublishAddKillLookup10_1_Test(cc *C) {
	doPublishAddKillLookup(7, 10, 20, cc)
}
