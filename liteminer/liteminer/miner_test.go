package liteminer

import (
	"math"
	"testing"
	"time"
)

func checkMineResults(t *testing.T, m *Miner, data string,
	lower, upper, expectedNonce uint64) {
	nonce, done := m.Mine(data, lower, upper)
	if !done {
		t.Errorf("Miner did not complete mining: %v", m)
	}
	if nonce != expectedNonce {
		t.Errorf("Expected nonce %d, but received %d", expectedNonce, nonce)
	}
}

func TestMiner(t *testing.T) {
	//PrintHashes("memezzz", math.MaxUint64-500, math.MaxUint64)

	Out.Printf("")
	Out.Printf("Starting TestMiner...")
	Out.Printf("")

	/* mining result tests */
	p, _ := CreateTestingPool(t)
	m := CreateTestingMiner(t, p)
	// - normal operation
	checkMineResults(t, m, "c", 0, 100, 3)
	checkMineResults(t, m, "abcdefhizjk", 0, 150, 89)
	checkMineResults(t, m, "lolz", 147, 150, 148)
	checkMineResults(t, m, "hello world", 0, 5, 4)
	checkMineResults(t, m, ")(#%)(*#%&^%)", 13500, 1000000, 81780)

	// - bounds checking
	checkMineResults(t, m, "c", 0, 3, 2)
	checkMineResults(t, m, "c", 2, 3, 2)
	// - weird intervals
	checkMineResults(t, m, "memezzz", math.MaxUint64-500, math.MaxUint64, 18446744073709551598)
	checkMineResults(t, m, "c", 0, 0, 0)
	checkMineResults(t, m, "c", 0, 1, 0)
	// - empty string data
	checkMineResults(t, m, "", 0, 100, 35)

	/* premature shutdown test */
	p, _ = CreateTestingPool(t)
	m = CreateTestingMiner(t, p)
	c := CreateTestingClient(p)
	finish_chan := make(chan int)

	go func() {
		time.Sleep(HEARTBEAT_FREQ + 500)
		if !m.Mining {
			t.Errorf("Miner was not mining when expected: %v", m)
		}
		if m.IsShutdown {
			t.Errorf("Miner was shutdown when NOT expected: %v", m)
		}
		m.Shutdown()
		time.Sleep(100 * time.Millisecond)
		// (whether mining doesn't matter)
		if !m.IsShutdown {
			t.Errorf("Miner did not shutdown when expected: %v", m)
		}
	}()
	go func() {
		time.Sleep(2*HEARTBEAT_FREQ + 1000) // wait out next heartbeat
		StopMiningRequest(c, p)
		finish_chan <- 1
	}()

	// send mining request after creating goroutines
	SendMiningRequest(t, p, c, "cats", 1000000000000000000) // unfinishable
	<-finish_chan

	/* premature connection close */
	p, _ = CreateTestingPool(t)
	m = CreateTestingMiner(t, p)
	c = CreateTestingClient(p)
	finish_chan = make(chan int)

	go func() {
		time.Sleep(HEARTBEAT_FREQ + 500)
		if !m.Mining {
			t.Errorf("Miner was not mining when expected: %v", m)
		}
		if m.IsShutdown {
			t.Errorf("Miner was shutdown when NOT expected: %v", m)
		}
		res := m.conn.Conn.Close()
		time.Sleep(100 * time.Millisecond)
		// (whether mining doesn't matter)
		if res != nil {
			t.Errorf("Miner conn did not shutdown correctly: %v", m)
		}
		if m.conn.Conn.Close() == nil {
			t.Errorf("Miner conn did not shutdown correctly: %v", m)
		}
	}()
	go func() {
		time.Sleep(2*HEARTBEAT_FREQ + 1000) // wait out next heartbeat
		StopMiningRequest(c, p)
		finish_chan <- 1
	}()
	SendMiningRequest(t, p, c, "zoinks", 1000000000000000000) // unfinishable
	<-finish_chan

	/* unexpected close from pool */
	p, _ = CreateTestingPool(t)
	m = CreateTestingMiner(t, p)
	c = CreateTestingClient(p)
	finish_chan = make(chan int)

	go func() {
		time.Sleep(HEARTBEAT_FREQ + 500)
		if !m.Mining {
			t.Errorf("Miner was not mining when expected: %v", m)
		}
		if m.IsShutdown {
			t.Errorf("Miner was shutdown when NOT expected: %v", m)
		}
		poolConn := GetPoolMinerConn(p, m)
		res := poolConn.Conn.Close()
		// (whether mining doesn't matter)
		if res != nil {
			t.Errorf("Pool miner conn did not shutdown correctly: %v", m)
		}
		time.Sleep(500 * time.Millisecond)
		if poolConn.Conn.Close() == nil {
			t.Errorf("Pool miner conn did not shutdown correctly: %v", m)
		}
	}()
	go func() {
		time.Sleep(2*HEARTBEAT_FREQ + 1000) // wait out next heartbeat
		StopMiningRequest(c, p)
		finish_chan <- 1
	}()
	SendMiningRequest(t, p, c, "lolz", 1000000000000000000) // unfinishable
	<-finish_chan

	/* unexpected message to miner */
	p, _ = CreateTestingPool(t)
	m = CreateTestingMiner(t, p)
	c = CreateTestingClient(p)
	finish_chan = make(chan int)

	go func() {
		time.Sleep(HEARTBEAT_FREQ + 500)
		if !m.Mining {
			t.Errorf("Miner was not mining when expected: %v", m)
		}
		if m.IsShutdown {
			t.Errorf("Miner was shutdown when NOT expected: %v", m)
		}
		poolConn := GetPoolMinerConn(p, m)
		SendMsg(poolConn, ClientHelloMsg()) // unexpected message for miner
		time.Sleep(500 * time.Millisecond)
		if !m.Mining {
			t.Errorf("Miner was not mining when expected: %v", m)
		}
		if m.IsShutdown {
			t.Errorf("Miner was shutdown when NOT expected: %v", m)
		}
	}()
	go func() {
		time.Sleep(2*HEARTBEAT_FREQ + 1000) // wait out next heartbeat
		StopMiningRequest(c, p)
		finish_chan <- 1
	}()
	SendMiningRequest(t, p, c, "lolz", 1000000000000000000) // unfinishable
	<-finish_chan
}
