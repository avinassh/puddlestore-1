package tapestry

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestTAStuff(t *testing.T) { TestingT(t) }

type IDSuite struct{}

var _ = Suite(&IDSuite{})

func (s *IDSuite) TestTA_SharedPrefixLength1_Test(cc *C) {
	a, _ := PartialID("1")
	b, _ := PartialID("2")

	p := SharedPrefixLength(a, b)
	cc.Assert(p, Equals, 0)
}

func (s *IDSuite) TestTA_SharedPrefixLength2_Test(cc *C) {
	a, _ := PartialID("519BFDD559CB03792ECD7565AEA263D08F3CF528")
	b, _ := PartialID("519BFDD559CB03792ECD7565AEA263D08F3CF528")

	p := SharedPrefixLength(a, b)

	cc.Assert(p, Equals, 40)
}

func (s *IDSuite) TestTA_SharedPrefixLength3_Test(cc *C) {
	a, _ := PartialID("8643ADD0432D58CA043FFF23C87FDF07F460B8FA")
	b, _ := PartialID("8643ADD0432D58CA04AFFF23C87FDF07F460B8FA")

	p := SharedPrefixLength(a, b)

	cc.Assert(p, Equals, 18)
}

func (s *IDSuite) TestTA_SharedPrefixLength4_Test(cc *C) {
	a, _ := PartialID("54D31843077B586455EAF042590739FBDAE8AA84")
	b, _ := PartialID("A4D31843077B586455EAF042590739FBDAE8AA84")

	p := SharedPrefixLength(a, b)

	cc.Assert(p, Equals, 0)
}

func (s *IDSuite) TestTA_BetterChoice1_Test(cc *C) {
	a, _ := PartialID("3")
	b, _ := PartialID("7")
	c, _ := PartialID("7")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, false)
}

func (s *IDSuite) TestTA_BetterChoice2_Test(cc *C) {
	a, _ := PartialID("00000007")
	b, _ := PartialID("00000007")
	c, _ := PartialID("00000007")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, false)
}

func (s *IDSuite) TestTA_BetterChoice3_Test(cc *C) {
	a, _ := PartialID("29283207")
	b, _ := PartialID("29283203")
	c, _ := PartialID("29283207")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, false)
}

func (s *IDSuite) TestTA_BetterChoice4_Test(cc *C) {
	a, _ := PartialID("29927")
	b, _ := PartialID("29927")
	c, _ := PartialID("29923")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, true)
}

func (s *IDSuite) TestTA_BetterChoice5_Test(cc *C) {
	a, _ := PartialID("23257")
	b, _ := PartialID("23251")
	c, _ := PartialID("23256")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, true)
}

func (s *IDSuite) TestTA_BetterChoice6_Test(cc *C) {
	a, _ := PartialID("1234567")
	b, _ := PartialID("1234561")
	c, _ := PartialID("1234562")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, true)
}

func (s *IDSuite) TestTA_BetterChoice7_Test(cc *C) {
	a, _ := PartialID("1")
	b, _ := PartialID("2")
	c, _ := PartialID("3")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, true)
}

func (s *IDSuite) TestTA_BetterChoice8_Test(cc *C) {
	a, _ := PartialID("1")
	b, _ := PartialID("3")
	c, _ := PartialID("2")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, false)
}

func (s *IDSuite) TestTA_BetterChoice9_Test(cc *C) {
	a, _ := PartialID("3")
	b, _ := PartialID("1")
	c, _ := PartialID("2")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, true)
}

func (s *IDSuite) TestTA_BetterChoice10_Test(cc *C) {
	a, _ := PartialID("3")
	b, _ := PartialID("2")
	c, _ := PartialID("1")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, false)
}

func (s *IDSuite) TestTA_BetterChoice11_Test(cc *C) {
	a, _ := PartialID("2")
	b, _ := PartialID("1")
	c, _ := PartialID("3")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, false)
}

func (s *IDSuite) TestTA_BetterChoice12_Test(cc *C) {
	a, _ := PartialID("2")
	b, _ := PartialID("3")
	c, _ := PartialID("1")

	better := a.BetterChoice(b, c)

	cc.Assert(better, Equals, true)
}

func (s *IDSuite) TestTA_Closer1_Test(cc *C) {
	a, _ := PartialID("5555555")
	b, _ := PartialID("5555556")
	c, _ := PartialID("5555554")

	closer := a.Closer(b, c)

	cc.Assert(closer, Equals, false)
}

func (s *IDSuite) TestTA_Closer2_Test(cc *C) {
	a, _ := PartialID("5555555")
	b, _ := PartialID("5555556")
	c, _ := PartialID("5555554")

	closer := a.Closer(c, b)

	cc.Assert(closer, Equals, false)
}

func (s *IDSuite) TestTA_Closer3_Test(cc *C) {
	a, _ := PartialID("5555555")
	b, _ := PartialID("555555F")
	c, _ := PartialID("5555554")

	closer := a.Closer(c, b)

	cc.Assert(closer, Equals, true)
}

func (s *IDSuite) TestTA_Closer4_Test(cc *C) {
	a, _ := PartialID("5555555")
	b, _ := PartialID("5555559")
	c, _ := PartialID("5555554")

	closer := a.Closer(b, c)

	cc.Assert(closer, Equals, false)
}

func (s *IDSuite) TestTA_Closer5_Test(cc *C) {
	a, _ := PartialID("0FFFFFFF")
	b, _ := PartialID("0FFFFFFD")
	c, _ := PartialID("10000000")

	closer := a.Closer(b, c)

	cc.Assert(closer, Equals, false)
}

func (s *IDSuite) TestTA_Closer6_Test(cc *C) {
	a, _ := PartialID("0FFFFFFF")
	b, _ := PartialID("0FFFFFFD")
	c, _ := PartialID("10000000")

	closer := a.Closer(c, b)

	cc.Assert(closer, Equals, true)
}

func (s *IDSuite) TestTA_Closer7_Test(cc *C) {
	a, _ := PartialID("0FFFFFFF")
	b, _ := PartialID("0FFFFFFE")
	c, _ := PartialID("10000000")

	closer := a.Closer(c, b)

	cc.Assert(closer, Equals, false)
}

func (s *IDSuite) TestTA_Closer8_Test(cc *C) {
	a, _ := PartialID("0FFFFFFF")
	b, _ := PartialID("0FFFFFFE")
	c, _ := PartialID("10000000")

	closer := a.Closer(b, c)

	cc.Assert(closer, Equals, false)
}
