package liteminer

import (
	"math"
	"net"
	"testing"
	"time"
)

// NOTE: this file has the _test prefix just so the coverage tool doesn't notice it

// CreateTestingPool wraps the code to create and check a pool
func CreateTestingPool(t *testing.T) (*Pool, string) {
	p, err := CreatePool("")
	if err != nil {
		t.Errorf("Received error %v when creating pool", err)
	}
	PoolFree(p, t)
	return p, p.Addr.String()
}

// PoolContainsNMiners checks whether the pool contains the given number of miners
func PoolContainsNMiners(p *Pool, n int, t *testing.T) {
	p.mutex.Lock()
	if len(p.Miners) != n {
		t.Errorf("incorrect # of miners: %v instead of %v", len(p.Miners), n)
	}
	p.mutex.Unlock()
}

// PoolFree checks all the properties of a pool when it is currently not connected to any client
func PoolFree(p *Pool, t *testing.T) {
	p.mutex.Lock()
	if p.busy {
		t.Errorf("Pool incorrectly busy")
	}
	if p.Data != "" {
		t.Errorf("Pool contains data in waiting state")
	}
	if p.Client.Conn != nil {
		Out.Printf("MiningConn is not nil")
		t.Errorf("MiningConn is not nil")
	}
	if len(p.OpenJobs) > 0 {
		t.Errorf("OpenJobs list not empty in waiting state")
	}
	if len(p.CompletedJobs) > 0 {
		t.Errorf("CompletedJobs not empty in waiting state")
	}
	p.mutex.Unlock()
}

// PoolWaiting checks all properties of a pool when it is connected to a client but not mining
func PoolWaiting(p *Pool, t *testing.T) {
	p.mutex.Lock()
	if p.busy {
		t.Errorf("Pool incorrectly busy")
	}
	if p.Data != "" {
		t.Errorf("Pool contains data in waiting state")
	}
	if p.Client.Conn == nil {
		Out.Printf("MiningConn is nil")
		t.Errorf("MiningConn is nil")
	}
	if len(p.OpenJobs) > 0 {
		t.Errorf("OpenJobs list not empty in waiting state")
	}
	if len(p.CompletedJobs) > 0 {
		t.Errorf("CompletedJobs not empty in waiting state")
	}
	p.mutex.Unlock()
}

// PoolInProcessingState checks all properties of a pool when it is
// currently processing a transaction
func PoolInProcessingState(p *Pool, c *Client, t *testing.T) {
	p.mutex.Lock()
	if !p.busy {
		t.Errorf("Pool not busy while processing transaction")
	}
	if p.Client.Conn == nil {
		t.Errorf("MiningConn null while in transaction")
	}

	_, err := c.GetConn(p.Client.Conn.LocalAddr())
	if err != nil {
		t.Errorf("client does not contain pool")
	}

	p.mutex.Unlock()
}

// PoolTestProcessingUntilComplete hangs until the client receives a nonce from
// pool. Checks that the pool is processing at every stage.
// NOTE: can't be called if client is disconnected
func PoolTestProcessingUntilComplete(p *Pool, c *Client, t *testing.T) {
	for {
		c.mutex.Lock()

		for addr, nonce := range c.Nonces {
			if addr.String() == p.Client.Conn.LocalAddr().String() && nonce != 0 {
				c.mutex.Unlock()
				return
			}
		}

		c.mutex.Unlock()
		PoolInProcessingState(p, c, t)
		time.Sleep(time.Millisecond * 50)
	}
}

// PoolTestProcessingUntilCompleteClientless is the same as PoolTestProcessingUntilComplete
// but doesn't require the Client be connected.
func PoolTestProcessingUntilCompleteClientless(p *Pool, c *Client, t *testing.T) {
	for p.busy {
		PoolInProcessingState(p, c, t)
		time.Sleep(time.Millisecond * 50)
	}
}

// GetPoolMinerConn returns the MiningConn for the given miner
// that is connected to that miner in the pool (the pool side of the connection)
func GetPoolMinerConn(p *Pool, m *Miner) MiningConn {
	addrs := p.GetMiners()

	for _, addr := range addrs {
		if addr.String() == m.conn.Conn.LocalAddr().String() {
			return p.Miners[addr]
		}
	}
	return MiningConn{nil, nil, nil}
}

// GetPoolMinerConn returns the MiningConn for the given miner
// that is connected to that miner in the pool (the pool side of the connection)
func GetClientPoolConn(c *Client, p *Pool) MiningConn {
	for addr, conn := range c.PoolConns {
		if addr.String() == p.Client.Conn.LocalAddr().String() {
			return conn
		}
	}
	return MiningConn{nil, nil, nil}
}

// CreateTestingMiner wraps the code to create and check a miner
func CreateTestingMiner(t *testing.T, p *Pool) *Miner {
	m, err := CreateMiner(p.Addr.String())
	if err != nil {
		t.Errorf("Received error %v when creating miner", err)
	}
	return m
}

// CreateTestingClient wraps the code to create and check a client
func CreateTestingClient(p *Pool) *Client {
	client := CreateClient([]string{p.Addr.String()})
	return client
}

// SendMiningRequest sends a mining request to the given pool through the given client
func SendMiningRequest(t *testing.T, p *Pool, c *Client, data string, upper uint64) (int64, error) {
	nonces, err := c.Mine(data, upper)
	if err != nil {
		t.Errorf("Received error %v when mining", err)
		return 0, err
	} else {
		return nonces[p.Addr], nil
	}
}

func SendMiningRequestAsync(t *testing.T, c *Client, data string, upper uint64) {
	go func() {
		_, err := c.Mine(data, upper)
		if err != nil {
			t.Errorf("Received error %v when mining", err)
		}
	}()
}

// stops client from waiting on a pool
func StopMiningRequest(c *Client, p *Pool) {
	c.TxResults <- map[net.Addr]int64{p.Addr: 0}
}

// simple user helper function to help generate test cases
// by printing out all hashes possible
func PrintHashes(data string, lower uint64, upper uint64) {
	Out.Printf("Testing hashes of '%s'", data)
	bestHash := uint64(math.MaxUint64)
	bestNonce := lower
	for n := lower; n < upper; n++ {
		curHash := Hash(data, n)
		Out.Printf("%d: %d", n, curHash)
		if curHash <= bestHash {
			bestHash = curHash
			bestNonce = uint64(n)
		}
	}
	Out.Printf("Best: %d: %d", bestNonce, bestHash)
}
