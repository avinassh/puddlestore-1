package liteminer

import (
	"reflect"
	"testing"
)

func TestInterval(t *testing.T) {

	NUM_ITERS := 100

	Out.Printf("TestInterval:")

	// tests that numIntervals=0 always returns empty
	for i := 0; i < NUM_ITERS; i++ {
		ivls := GenerateIntervals(uint64(i), 0)
		if len(ivls) != 0 {
			t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
				i, 0, make([]Interval, 0), ivls)
		}
	}

	// tests case where upperBound = numIntervals

	ivls := GenerateIntervals(uint64(1), 1)
	expected := make([]Interval, 1)
	expected[0] = Interval{0, 2, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			1, 1, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(2), 2)
	expected = make([]Interval, 2)
	expected[0] = Interval{0, 2, 0, 0}
	expected[1] = Interval{2, 3, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			2, 2, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(3), 3)
	expected = make([]Interval, 3)
	expected[0] = Interval{0, 2, 0, 0}
	expected[1] = Interval{2, 3, 0, 0}
	expected[2] = Interval{3, 4, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			3, 3, expected, ivls)
	}

	// tests case where upperBound = numIntervals + 1 (intervals are all evenly distributed)

	ivls = GenerateIntervals(uint64(2), 1)
	expected = make([]Interval, 1)
	expected[0] = Interval{0, 3, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			2, 1, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(3), 2)
	expected = make([]Interval, 2)
	expected[0] = Interval{0, 2, 0, 0}
	expected[1] = Interval{2, 4, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			1, 1, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(4), 3)
	expected = make([]Interval, 3)
	expected[0] = Interval{0, 2, 0, 0}
	expected[1] = Interval{2, 4, 0, 0}
	expected[2] = Interval{4, 5, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			1, 1, expected, ivls)
	}

	// tests case where upperBound >> numIntervals

	ivls = GenerateIntervals(uint64(9), 3)
	expected = make([]Interval, 3)
	expected[0] = Interval{0, 4, 0, 0}
	expected[1] = Interval{4, 7, 0, 0}
	expected[2] = Interval{7, 10, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			9, 3, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(9), 2)
	expected = make([]Interval, 2)
	expected[0] = Interval{0, 5, 0, 0}
	expected[1] = Interval{5, 10, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			9, 2, expected, ivls)
	}

	// tests case where upperBound < numIntervals
	ivls = GenerateIntervals(uint64(0), 1)
	expected = make([]Interval, 1)
	expected[0] = Interval{0, 1, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			0, 1, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(0), 3)
	expected = make([]Interval, 3)
	expected[0] = Interval{0, 1, 0, 0}
	expected[1] = Interval{1, 1, 0, 0}
	expected[2] = Interval{1, 1, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			0, 3, expected, ivls)
	}

	ivls = GenerateIntervals(uint64(2), 4)
	expected = make([]Interval, 4)
	expected[0] = Interval{0, 1, 0, 0}
	expected[1] = Interval{1, 2, 0, 0}
	expected[2] = Interval{2, 3, 0, 0}
	expected[3] = Interval{3, 3, 0, 0}
	if !reflect.DeepEqual(ivls, expected) {
		t.Errorf("GenerateIntervals failed with %v %v: expected %v, received %v",
			2, 4, expected, ivls)
	}

}
