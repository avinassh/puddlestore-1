/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines IDs for tapestry and provides various utility functions
 *  for manipulating and creating them. Provides functions to compare IDs
 *  for insertion into routing tables, and for implementing the routing
 *  algorithm.
 */

package tapestry

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"time"
)

// An ID is a digit array
type ID [DIGITS]Digit

// A digit is just a typedef'ed uint8
type Digit uint8

// Random number generator for generating random node ID
var random = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

// Returnes a random ID.
func RandomID() ID {
	var id ID
	for i := range id {
		id[i] = Digit(random.Intn(BASE))
	}
	return id
}

// Hashes the string to an ID
func Hash(key string) (id ID) {
	// Sha-hash the key
	sha := sha1.New()
	sha.Write([]byte(key))
	hash := sha.Sum([]byte{})

	// Store in an ID
	for i := range id {
		id[i] = Digit(hash[(i/2)%len(hash)])
		if i%2 == 0 {
			id[i] >>= 4
		}
		id[i] %= BASE
	}

	return id
}

// Returns the length of the prefix that is shared by the two IDs.
func SharedPrefixLength(a ID, b ID) (i int) {
	// __BEGIN_TA__
	for ; i < DIGITS; i++ {
		if a[i] != b[i] {
			break
		}
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this function
	// __END_STUDENT__
	return
}

// Used by Tapestry's surrogate routing.  Given IDs first and second, which is the better choice?
//
// The "better choice" is the ID that:
//  - has the longest shared prefix with id
//  - if both have prefix of length n, which id has a better (n+1)th digit?
//  - if both have the same (n+1)th digit, consider (n+2)th digit, etc.

// Returns true if the first ID is the better choice.
// Returns false if second ID is closer or if first == second.
func (id ID) BetterChoice(first ID, second ID) bool {
	// __BEGIN_TA__
	for i, digit := range id {
		if first[i] != second[i] {
			delta_first := (first[i] - digit) % BASE
			delta_second := (second[i] - digit) % BASE
			return delta_first < delta_second
		}
	}
	// If we get here, a and b are the same, so a is NOT closer than b
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return false
}

// Used when inserting nodes into Tapestry's routing table.  If the routing
// table has multiple candidate nodes for a slot, then it chooses the node that
// is closer to the local node.
//
// In a production Tapestry implementation, closeness is determined by looking
// at the round-trip-times (RTTs) between (a, id) and (b, id); the node with the
// shorter RTT is closer.
//
// In this implementation, we have decided to define closeness as the absolute
// value of the difference between a and b. This is NOT the same as your
// implementation of BetterChoice.
//
// Return true if a is closer than b.
// Return false if b is closer than a, or if a == b.
func (id ID) Closer(first ID, second ID) bool {
	// __BEGIN_TA__
	big_first := first.big()
	big_second := second.big()
	big_id := id.big()

	diff_first := big_first.Sub(big_first, big_id)
	diff_second := big_second.Sub(big_second, big_id)

	abs_first := diff_first.Abs(diff_first)
	abs_second := diff_second.Abs(diff_second)

	if abs_first.Cmp(abs_second) == -1 {
		return true
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return false
}

// Helper function: convert an ID to a big int.
func (id ID) big() (b *big.Int) {
	b = big.NewInt(0)
	base := big.NewInt(BASE)
	for _, digit := range id {
		b.Mul(b, base)
		b.Add(b, big.NewInt(int64(digit)))
	}
	return b
}

// String representation of an ID is hexstring of each digit.
func (id ID) String() string {
	var buf bytes.Buffer
	for _, d := range id {
		buf.WriteString(d.String())
	}
	return buf.String()
}

// String representation of a digit is its hex value
func (digit Digit) String() string {
	return fmt.Sprintf("%X", byte(digit))
}

func (i ID) bytes() []byte {
	b := make([]byte, len(i))
	for idx, d := range i {
		b[idx] = byte(d)
	}
	return b
}

func idFromBytes(b []byte) (i ID) {
	if len(b) < DIGITS {
		return
	}
	for idx, d := range b[:DIGITS] {
		i[idx] = Digit(d)
	}
	return
}

// Parse an ID from String
func ParseID(stringID string) (ID, error) {
	var id ID

	if len(stringID) != DIGITS {
		return id, fmt.Errorf("Cannot parse %v as ID, requires length %v, actual length %v", stringID, DIGITS, len(stringID))
	}

	for i := 0; i < DIGITS; i++ {
		d, err := strconv.ParseInt(stringID[i:i+1], 16, 0)
		if err != nil {
			return id, err
		}
		id[i] = Digit(d)
	}

	return id, nil
}
