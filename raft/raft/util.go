package raft

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"os"
	"time"
)

func min(num1 uint64, num2 uint64) (res uint64) {
	if num1 <= num2 {
		return num1
	} else {
		return num2
	}
}

// OpenPort creates a listener on the specified port.
func OpenPort(port int) (net.Listener, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	conn, err := net.Listen("tcp4", fmt.Sprintf("%v:%v", hostname, port))
	return conn, err
}

// AddrToId converts a network address to a Raft node ID of specified length.
func AddrToId(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	v := h.Sum(nil)
	keyInt := big.Int{}
	keyInt.SetBytes(v[:length])
	return keyInt.String()
}

// randomTimeout uses time.After to create a timeout between minTimeout and 2x that.
func (r *RaftNode) randomTimeout(minTimeout time.Duration) <-chan time.Time {
	minNanos := minTimeout.Nanoseconds()
	// random number between 0 and minNanos representing the amount of time to add
	extraTime := rand.Int63n(minNanos)
	r.Verbose("started timer for %d ns", minNanos+extraTime)
	return time.After(time.Nanosecond * time.Duration(minNanos+extraTime))
}

// createCacheId creates a unique ID to store a client request and corresponding
// reply in cache.
func createCacheId(clientId, sequenceNum uint64) string {
	return fmt.Sprintf("%v-%v", clientId, sequenceNum)
}

// UInt64Slice is a type definition for a slice of uint64s. We then define
// functions Len, Swap, and Less on it in order to implement sort.Interface
// and thus enable sorting a uint64 slice.
//
// See https://golang.org/pkg/sort/ for more details. Note that this is no
// longer required starting Go 1.8, but we're using Go 1.7 as the current
// official version for this class.
type UInt64Slice []uint64

func (p UInt64Slice) Len() int {
	return len(p)
}

func (p UInt64Slice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p UInt64Slice) Less(i, j int) bool {
	return p[i] < p[j]
}
