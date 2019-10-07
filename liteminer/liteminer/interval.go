/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: contains all interval related logic.
 */

package liteminer

// Interval represents [Lower, Upper)
type Interval struct {
	Lower uint64 // Inclusive
	Upper uint64 // Exclusive
	Nonce uint64
	Hash  uint64
}

// GenerateIntervals divides the range [0, upperBound] into numIntervals
// intervals. Always returns numIntervals intervals, even if these are empty intervals.
func GenerateIntervals(upperBound uint64, numIntervals int) (intervals []Interval) {

	if numIntervals < 1{
		return
	}

	// now upperBound = # of discrete numbers that must be shared
	upperBound++
	ui := uint64(numIntervals)
	// does integer division, rounds down
	intervalSize := upperBound / ui
	// calculates remainder - this should be <= numIntervals, distribute this evenly
	leftOver := upperBound % ui

	// keeps track of current lower
	lower := uint64(0)
	for n := uint64(0); n < ui; n++ {
		upper := lower + intervalSize
		if leftOver > 0 {
			upper++
			leftOver--
		}
		i := Interval{lower, upper, 0, 0}
		intervals = append(intervals, i)
		lower = upper
	}
	return
}
