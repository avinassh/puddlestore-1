/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: a LiteMiner miner.
 */

package liteminer

import (
	"fmt"
	"io"
	"math"
	"sync"
	"time"
	"net"
)

const HEARTBEAT_FREQ = 1000 * time.Millisecond

// Represents a LiteMiner miner
type Miner struct {
	IsShutdown   bool
	Mining       bool
	NumProcessed uint64     // Number of values processed in the current mining range
	mutex        sync.Mutex // To manage concurrent access to these members
	conn         MiningConn // for testing
	Beat	     bool 		// for testing
}

// CreateMiner creates a new miner connected to the pool at the specified address.
func CreateMiner(addr string) (mp *Miner, err error) {
	var miner Miner

	mp = &miner

	miner.Mining = false
	miner.NumProcessed = 0
	miner.IsShutdown = false
	miner.Beat = true

	err = miner.connect(addr)

	return
}

// connect connects the miner to the pool at the specified address.
func (m *Miner) connect(addr string) (err error) {
	conn, err := MinerConnect(addr)
	if err != nil {
		return fmt.Errorf("Received error %v when connecting to pool %v\n", err, addr)
	}

	m.mutex.Lock()
	Debug.Printf("Connected to pool %v", addr)
	m.conn = conn
	m.IsShutdown = false
	m.mutex.Unlock()

	go m.receiveFromPool(conn)
	go m.sendHeartBeats(conn)

	return
}

// receiveFromPool processes messages from the pool represented by conn.
func (m *Miner) receiveFromPool(conn MiningConn) {
	for {
		m.mutex.Lock()
		// reset values
		m.Mining = false
		m.NumProcessed = 0

		if m.IsShutdown {
			conn.Conn.Close() // Close the connection
			m.mutex.Unlock()
			return
		}
		m.mutex.Unlock()

		msg, err := RecvMsg(conn)
		if err != nil {
			if _, ok := err.(*net.OpError); ok || err == io.EOF {
				Err.Printf("Lost connection to pool %v\n", conn.Conn.RemoteAddr())
				conn.Conn.Close() // Close the connection
				return
			}

			Err.Printf(
				"Received error %v when processing pool %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)
			continue
		}

		if msg.Type != MineRequest {
			Err.Printf(
				"Received unexpected message of type %v from pool %v\n",
				msg.Type,
				conn.Conn.RemoteAddr(),
			)
		}

		Debug.Printf("Received MineRequest message")
		nonce, done := m.Mine(msg.Data, msg.Lower, msg.Upper) // Service the mine request

		if done {
			Debug.Printf("Finished mining; found min nonce %d", nonce)

			// Send result
			res := ProofOfWorkMsg(msg.Data, nonce, Hash(msg.Data, nonce))
			SendMsg(conn, res)
		} else {
			Debug.Printf("Mining halted")
		}
	}
}

// sendHeartBeats periodically sends heartbeats to the pool represented by conn
// while mining. sendHeartBeats should NOT send heartbeats to the pool if the
// miner is not mining. It should close the connection and return if the miner
// is shutdown.
func (m *Miner) sendHeartBeats(conn MiningConn) {
	tickCh := time.Tick(HEARTBEAT_FREQ)

	// note: because this function loops until shutdown and
	// only heartbeats when mining, there are no race conditions
	// with mining start

	for {
		// hangs until a tick is sent over the channel
		<-tickCh

		// grab variables quickly so we don't interrupt mining
		m.mutex.Lock()
		shutdown := m.IsShutdown
		processed := m.NumProcessed
		mining := m.Mining
		beat := m.Beat
		m.mutex.Unlock()

		if shutdown {
			conn.Conn.Close()
			return
		} else if mining && beat {
			Debug.Printf("Sent heartbeat (numProcessed=%d)", processed)
			SendMsg(conn, StatusUpdateMsg(processed))
		}
		// do nothing and continue if we're not mining (we may in the future)
	}
}

// Given a data string, a lower bound (inclusive), and an upper bound
// (exclusive), Mine returns the nonce in the range [lower, upper) that
// corresponds to the lowest hash value. With each value processed in the range,
// NumProcessed should increase.
func (m *Miner) Mine(data string, lower, upper uint64) (nonce uint64, done bool) {

	done = true

	// mining is starting
	m.mutex.Lock()
	m.Mining = true
	m.mutex.Unlock()

	Out.Printf("Miner %v mining from %d-%d to find %s", m.conn.Conn.LocalAddr(), lower, upper, data)
	bestHash := uint64(math.MaxUint64)
	nonce = lower
	for n := lower; n < upper; n++ {
		if curHash := Hash(data, n); curHash <= bestHash {
			bestHash = curHash
			nonce = n
		}

		m.mutex.Lock()
		m.NumProcessed++
		// if we were shut down, stop mining
		if m.IsShutdown {
			done = false
			m.mutex.Unlock()
			return
		}
		m.mutex.Unlock()
	}

	m.mutex.Lock()
	m.Mining = false
	m.mutex.Unlock()

	return
}

// Stop sending heartbeats
func (m *Miner) StopBeating() {
	m.mutex.Lock()
	m.Beat = false
	m.mutex.Unlock()
}

// Start sending heartbeats
func (m *Miner) StartBeating() {
	m.mutex.Lock()
	m.Beat = true
	m.mutex.Unlock()
}

// Shutdown marks the miner as shutdown and asynchronously disconnects it from
// its pool.
func (m *Miner) Shutdown() {
	m.mutex.Lock()
	Debug.Printf("Shutting down")
	m.IsShutdown = true
	m.mutex.Unlock()
}
