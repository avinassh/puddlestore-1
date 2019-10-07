package raft

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	// __BEGIN_TA__
	"math/rand"
	// __END_TA__
	"net"
	"os"
	"time"
)

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
func randomTimeout(minTimeout time.Duration) <-chan time.Time {
	// __BEGIN_TA__
	extra := (time.Duration(rand.Int63()) % minTimeout)
	return time.After(minTimeout + extra)
	// __END_TA__
	/* __BEGIN_STUDENT__
	// TODO: Students should implement this method
	return nil
	__END_STUDENT__ */
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
