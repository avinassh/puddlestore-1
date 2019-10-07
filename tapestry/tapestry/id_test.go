package tapestry

import (
	"testing"
)

type IDTest struct {
	nodes   []ID
	objects map[ID]int
}

// Create a new blobstore
func NewIDTest(t *testing.T) *IDTest {
	it := new(IDTest)
	it.nodes = make([]ID, 0, 4)
	it.nodes = append(it.nodes, CreateIDForTesting(t, "583f"))
	it.nodes = append(it.nodes, CreateIDForTesting(t, "70d1"))
	it.nodes = append(it.nodes, CreateIDForTesting(t, "70f5"))
	it.nodes = append(it.nodes, CreateIDForTesting(t, "70fa"))

	// objects mapping ID to index of ID they map to
	it.objects = make(map[ID]int)
	it.objects[CreateIDForTesting(t, "3f8a")] = 0
	it.objects[CreateIDForTesting(t, "520c")] = 0
	it.objects[CreateIDForTesting(t, "58ff")] = 0
	it.objects[CreateIDForTesting(t, "70c3")] = 1
	it.objects[CreateIDForTesting(t, "60f4")] = 2
	it.objects[CreateIDForTesting(t, "70a2")] = 1
	it.objects[CreateIDForTesting(t, "6395")] = 1
	it.objects[CreateIDForTesting(t, "683f")] = 1
	it.objects[CreateIDForTesting(t, "63e5")] = 2
	it.objects[CreateIDForTesting(t, "63e9")] = 3
	it.objects[CreateIDForTesting(t, "beef")] = 0
	return it
}

func CreateIDForTesting(t *testing.T, id string) (ID ID) {
	ID, err := ParseID(id)
	if err != nil {
		ErrorPrintf(t, "error while creating node with ID %v", id)
	}
	return
}

func TestID_BetterChoice(t *testing.T) {
	if DIGITS == 4 && BASE >= 16 {
		it := NewIDTest(t)
		for o, target := range it.objects {
			for _, n := range it.nodes {
				if o.BetterChoice(n, it.nodes[target]) {
					ErrorPrintf(t, "%v mapped to %v instead of %v", o.String(), n.String(), it.nodes[target].String())
				}
			}
		}

		id, _ := ParseID("6d69")
		id2, _ := ParseID("b845")
		id3, _ := ParseID("b19f")
		if id.BetterChoice(id2, id3) {
			t.Errorf("failed lol")
		}
	} else {
		t.Errorf("Incorrect DIGIT and BASE setting (need DIGIT = 4, BASE > 16)")
	}
}

func TestID_Closer(t *testing.T) {
	if DIGITS == 4 && BASE >= 16 {
		it := NewIDTest(t)
		n := it.nodes
		s := true
		s = s && n[0].Closer(n[0], n[1])
		s = s && n[0].Closer(n[0], n[2])
		s = s && n[0].Closer(n[0], n[3])
		s = s && n[1].Closer(n[1], n[0])
		s = s && !n[1].Closer(n[2], n[2])

		if !s {
			ErrorPrintf(t, "test failed")
		}
	} else {
		t.Errorf("Incorrect DIGIT and BASE setting (need DIGIT = 4, BASE > 16)")
	}
}

func TestSharedPrefixLength(t *testing.T) {
	if DIGITS == 4 && BASE >= 16 {

		it := *NewIDTest(t)

		// 0 case
		for i := 1; i < 4; i++ {
			if p := SharedPrefixLength(it.nodes[0], it.nodes[i]); p != 0 {
				ErrorPrintf(t, "%v, %v had %v prefix length", it.nodes[0], it.nodes[i], p)
			}
		}

		// 2 case
		for i := 2; i < 4; i++ {
			if p := SharedPrefixLength(it.nodes[1], it.nodes[i]); p != 2 {
				ErrorPrintf(t, "%v, %v had %v prefix length", it.nodes[1], it.nodes[i], p)
			}
		}

		// 3 case
		for i := 3; i < 4; i++ {
			if p := SharedPrefixLength(it.nodes[2], it.nodes[i]); p != 3 {
				ErrorPrintf(t, "%v, %v had %v prefix length", it.nodes[2], it.nodes[i], p)
			}
		}

		// 4 case
		for i := 0; i < 4; i++ {
			if p := SharedPrefixLength(it.nodes[i], it.nodes[i]); p != 4 {
				ErrorPrintf(t, "%v, %v had %v prefix length", it.nodes[i], it.nodes[i], p)
			}
		}
	} else {
		t.Errorf("Incorrect DIGIT and BASE setting (need DIGIT = 4, BASE > 16)")
	}
}
