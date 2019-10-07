package tapestry

import (
	"bytes"
	"strconv"
	"testing"
)

type RTTest struct {
	table *RoutingTable
}

func NewRTTest(id string) *RTTest {
	rt := new(RTTest)
	parsed, _ := ParseID(id)
	n := RemoteNode{
		Id: parsed,
	}
	//temp node created in table
	rt.table = NewRoutingTable(n)
	return rt
}

// inspect these tests by eye, because they print output

func (rt *RTTest) NextHop(id string) string {
	i, _ := ParseID(id)
	return rt.table.GetNextHop(i).Id.String()
}

func TestRoutingTable_Add(t *testing.T) {
	if DIGITS == 4 && BASE >= 16 {
		rt := NewRTTest("3f93")
		Out.Printf(rt.table.String())
		// first row
		rt.table.Add(*RemoteNodeFromID("1c42"))
		rt.table.Add(*RemoteNodeFromID("2fe4"))
		rt.table.Add(*RemoteNodeFromID("f0d7"))
		// throws error when trying to add yourself
		rt.table.Add(*RemoteNodeFromID("3f93"))
		rt.table.Add(*RemoteNodeFromID("437e"))
		rt.table.Add(*RemoteNodeFromID("5c2a"))
		rt.table.Add(*RemoteNodeFromID("65bb"))
		rt.table.Add(*RemoteNodeFromID("705b"))
		rt.table.Add(*RemoteNodeFromID("d340"))
		rt.table.Add(*RemoteNodeFromID("e9ce"))
		rt.table.Add(*RemoteNodeFromID("8887"))
		rt.table.Add(*RemoteNodeFromID("93cb"))
		rt.table.Add(*RemoteNodeFromID("c3ca"))
		// second row
		rt.table.Add(*RemoteNodeFromID("309c"))
		rt.table.Add(*RemoteNodeFromID("362d"))
		rt.table.Add(*RemoteNodeFromID("3c6f"))
		// third row
		rt.table.Add(*RemoteNodeFromID("3f91"))

		// adds two duplicates in first row to fill up slotsize
		rt.table.Add(*RemoteNodeFromID("1111"))
		rt.table.Add(*RemoteNodeFromID("1112"))

		// add one more in same slot, it should not get added bc further
		add, node := rt.table.Add(*RemoteNodeFromID("1000"))
		if add || node != nil {
			ErrorPrintf(t, "1000 added incorrectly")
		}

		// add one more that should get added
		add, node = rt.table.Add(*RemoteNodeFromID("1113"))
		if !add || node.Id.String() != "1111" {
			ErrorPrintf(t, "1113 not added or incorrect node kicked")
		}

		Out.Printf(rt.table.String())
	} else {
		t.Errorf("Incorrect DIGIT and BASE setting (need DIGIT = 4, BASE > 16)")
	}
}

func TestRoutingTable_GetLevel(t *testing.T) {
	rt := NewRTTest("3f93")
	// zero row
	zero := []string{"1c42", "2fe4", "f0d7", "3f93", "437e", "5c2a", "65bb", "705b", "d340", "e9ce", "8887", "93cb",
		"c3ca"}
	for _, id := range zero {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	one := []string{"309c", "362d", "3c6f"}
	for _, id := range one {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	// second row
	rt.table.Add(*RemoteNodeFromID("3f81"))
	// three row
	rt.table.Add(*RemoteNodeFromID("3f91"))

	// adds two duplicates in first row to fill up slotsize
	rt.table.Add(*RemoteNodeFromID("1111"))
	rt.table.Add(*RemoteNodeFromID("1112"))

	Out.Print(rt.table.String())

	rows := make(map[int][]RemoteNode)
	for i := 0; i < DIGITS; i++ {
		rows[i] = rt.table.GetLevel(i)
	}

	for k, nodes := range rows {
		var buffer bytes.Buffer
		buffer.WriteString("row " + strconv.Itoa(k) + ": ")
		for _, node := range nodes {
			buffer.WriteString(node.Id.String() + ", ")
		}
		buffer.WriteString("\n")
		Out.Printf(buffer.String())
	}

	// inspect this to see what output is

}

func TestRoutingTable_Remove(t *testing.T) {
	rt := NewRTTest("3f93")
	// zero row
	zero := []string{"1c42", "2fe4", "f0d7", "437e", "5c2a", "65bb", "705b", "d340", "e9ce", "8887", "93cb",
		"c3ca"}
	for _, id := range zero {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	one := []string{"309c", "362d", "3c6f"}
	for _, id := range one {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	// second row
	rt.table.Add(*RemoteNodeFromID("3f81"))
	// three row
	rt.table.Add(*RemoteNodeFromID("3f91"))

	// adds two duplicates in first row to fill up slotsize
	rt.table.Add(*RemoteNodeFromID("1111"))
	rt.table.Add(*RemoteNodeFromID("1112"))

	Out.Print(rt.table.String())

	rows := make(map[int][]RemoteNode)
	for i := 0; i < DIGITS; i++ {
		rows[i] = rt.table.GetLevel(i)
	}

	for _, id := range zero {
		s := rt.table.Remove(*RemoteNodeFromID(id))
		if !s {
			ErrorPrintf(t, "could not be removed when present in table")
		}
	}

	Out.Printf(rt.table.String())
	// inspect this to see what output is

}

func TestRoutingTable_GetNextHop(t *testing.T) {
	rt := NewRTTest("3f93")

	// zero row
	zero := []string{"1c42", "2fe4", "f0d7", "3f93", "437e", "5c2a", "65bb", "705b", "d340", "e9ce", "8887", "93cb", "c3ca"}
	for _, id := range zero {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	one := []string{"309c", "362d", "3c6f"}
	for _, id := range one {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	two := []string{"3f71"}
	for _, id := range two {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	three := []string{"3f91"}
	for _, id := range three {
		rt.table.Add(*RemoteNodeFromID(id))
	}

	// adds two duplicates in first row to fill up slotsize
	rt.table.Add(*RemoteNodeFromID("1111"))
	rt.table.Add(*RemoteNodeFromID("1112"))

	Out.Printf(rt.table.String())

	routes := make(map[string]string)
	// one element is present at the correct place in the routing table
	routes["6666"] = "65BB"
	routes["6000"] = "65BB"
	routes["3C99"] = "3C6F"
	routes["3f80"] = "3F91"
	// many elements present, pick BestChoice
	routes["1FFF"] = "1111"
	routes["1C32"] = "1C42"
	routes["1110"] = "1111"
	// no elements present in current slot, pick the first node in the next slot
	routes["A999"] = "C3CA"
	routes["A000"] = "C3CA"
	routes["3EFF"] = "3F71"
	routes["319C"] = "362D"
	routes["3F71"] = "3F71"
	routes["3FFF"] = "3F71"

	// routes to itself (goes through the entire table)
	routes["3F92"] = "3F93"
	routes["3f93"] = "3F93"
	routes["3E92"] = "3F93"
	// routes to a mid-node and traverses a couple rows

	for req, rsp := range routes {
		if n := rt.NextHop(req); n != rsp {
			t.Errorf("%v routed to %v instead of %v", req, n, rsp)
		}
	}
}

func TestBetterChoiceBasic(t *testing.T) {
	id1, _ := ParseID("1112")
	id2, _ := ParseID("1111")
	id3, _ := ParseID("1FFF")
	if id3.BetterChoice(id2, id1) {
		Out.Printf("yay")
	} else {
		t.Errorf("Better choice failure")
	}
}
