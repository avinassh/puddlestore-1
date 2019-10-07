package liteminer

import (
	"testing"
	"time"
	"math"
)

// TestWaitingPoolDisconnectMiners tests if miners stay on the miner
// list if disconnected (from either end) when waiting for jobs
func TestWaitingPoolDisconnectMiners(t *testing.T) {
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)

	m1 := CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	m3 := CreateTestingMiner(t, p)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 3, t)
	PoolWaiting(p, t)

	// disconnect both ways before mining ends
	m3.conn.Conn.Close()
	m3.Shutdown() // so it doesn't freak out
	time.Sleep(HEARTBEAT_TIMEOUT+500) // long time to notice this
	PoolContainsNMiners(p, 3, t)

	GetPoolMinerConn(p, m1).Conn.Close()
	time.Sleep(HEARTBEAT_TIMEOUT+500) // long time to notice this
	PoolContainsNMiners(p, 3, t)

	m4 := CreateTestingMiner(t, p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 4, t)

	CreateTestingMiner(t, p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 5, t)

	GetPoolMinerConn(p, m4).Conn.Close()
	time.Sleep(HEARTBEAT_TIMEOUT+500) // long time to notice this
	PoolContainsNMiners(p, 5, t)

	// mine
	SendMiningRequestAsync(t, c, "kazooom", 100000)
	PoolContainsNMiners(p, 5, t)
	time.Sleep(10 * time.Millisecond)
	PoolTestProcessingUntilComplete(p, c, t)
	PoolWaiting(p, t)
	p.Client.Conn.Close()
	time.Sleep(10 * time.Millisecond)
	PoolFree(p, t)
}

func TestPoolWaitOnMiners(t *testing.T) {
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)

	m1 := CreateTestingMiner(t, p)
	m2 := CreateTestingMiner(t, p)
	m3 := CreateTestingMiner(t, p)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 3, t)
	PoolWaiting(p, t)

	SendMiningRequestAsync(t, c, "boom", 10000000)
	time.Sleep(5 * time.Millisecond)

	// disconnect current miners
	GetPoolMinerConn(p, m1).Conn.Close()
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 2, t)
	GetPoolMinerConn(p, m3).Conn.Close()
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 1, t)
	GetPoolMinerConn(p, m2).Conn.Close()
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 0, t)
	PoolInProcessingState(p, c, t)

	time.Sleep(50 * time.Millisecond)
	PoolInProcessingState(p, c, t)
	m1 = CreateTestingMiner(t, p)
	PoolInProcessingState(p, c, t)
	m2 = CreateTestingMiner(t, p)
	PoolInProcessingState(p, c, t)
	m3 = CreateTestingMiner(t, p)
	PoolInProcessingState(p, c, t)
	time.Sleep(1 * time.Millisecond)
	PoolContainsNMiners(p, 3, t)
	PoolTestProcessingUntilComplete(p, c, t)
	PoolWaiting(p, t)
	p.Client.Conn.Close()
	time.Sleep(10 * time.Millisecond)
	PoolFree(p, t)
}

func TestAddingMiners(t *testing.T) {
	// Tests that adding miners decreases OpenJobs
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 2, t)
	PoolWaiting(p, t)
	Out.Printf("two miners in waiting pool")

	SendMiningRequestAsync(t, c, "test1", 100000000000)
	Out.Printf("two miners working")
	time.Sleep(10*time.Millisecond)
	p.mutex.Lock()
	oj := len(p.OpenJobs)
	p.mutex.Unlock()
	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	time.Sleep(5*time.Millisecond)
	PoolContainsNMiners(p, 4, t)
	p.mutex.Lock()
	oj2 := len(p.OpenJobs)
	p.mutex.Unlock()

	Out.Printf("%v %v", oj, oj2)
	if oj2 >= oj {
		t.Errorf("No speedup experienced")
	}
}

func TestRemovingMiners(t *testing.T) {
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	m1 := CreateTestingMiner(t, p)
	m2 := CreateTestingMiner(t, p)
	c := CreateTestingClient(p)
	time.Sleep(20 * time.Millisecond)
	PoolContainsNMiners(p, 2, t)
	PoolWaiting(p, t)
	Out.Printf("two miners in waiting pool")

	SendMiningRequestAsync(t, c, "test1", math.MaxUint64 - 1) //unfinishable
	time.Sleep(5*time.Millisecond)
	p.mutex.Lock()
	if len(p.OpenJobs) != 2 {
		t.Errorf("job finished when it shouldn't have: %v instead of %v", len(p.OpenJobs), 2)
	}
	p.mutex.Unlock()
	Out.Printf("two miners working on job")

	m1.Shutdown()
	Out.Printf("one miner shutdown")
	time.Sleep(4*time.Second)
	PoolContainsNMiners(p, 1, t)
	p.mutex.Lock()
	if len(p.OpenJobs) != 3 {
		t.Errorf("jobs not reappropriated by pool: %v instead of %v", len(p.OpenJobs), 3)
	}
	p.mutex.Unlock()

	// all miners shutdown
	m2.Shutdown()
	Out.Printf("all miners shutdown")
	time.Sleep(5*time.Second)
	PoolContainsNMiners(p, 0, t)
	PoolInProcessingState(p, c, t)

	// checks that the job channel doesn't change
	p.mutex.Lock()
	if len(p.OpenJobs) != 4 && len(p.CompletedJobs) != 0 {
		t.Errorf("job finished when it shouldn't have")
	}
	p.mutex.Unlock()
	PoolInProcessingState(p, c, t)
	time.Sleep(5*time.Second)
	PoolInProcessingState(p, c, t)
	if len(p.OpenJobs) != 4 && len(p.CompletedJobs) != 0 {
		t.Errorf("fluctuation in job channels when no miner is present")
	}
	PoolInProcessingState(p, c, t)

	//reconnects all miners
	Out.Printf("new miners being generated")
	m1.connect(p.Addr.String())
	m2.connect(p.Addr.String())
	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	time.Sleep(20*time.Millisecond)
	Out.Printf("all miners generated")
	PoolContainsNMiners(p, 4, t)
	if len(p.OpenJobs) != 0 {
		t.Errorf("jobs not assigned correctly: %v instead of %v", len(p.OpenJobs), 0)
	}

}

func TestClientBarrage(t *testing.T) {
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)

	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 3, t)
	PoolFree(p, t)

	numClients := 25
	clients := make([]*Client, 0)
	for i := 0; i < numClients; i++ {
		clients = append(clients, CreateTestingClient(p))
	}
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p, t)

	numClientsConnected := 0
	clientConnected := new(Client)
	for _, client := range clients {
		if len(client.PoolConns) > 0 {
			numClientsConnected++
			clientConnected = client
		}
	}
	if numClientsConnected != 1 {
		t.Errorf("Unexpected number of clients connected: %d, %v", numClientsConnected, clients)
	}

	if numClientsConnected > 0 {
		// mine
		SendMiningRequestAsync(t, clientConnected, "kazooom", 100000)
		time.Sleep(10 * time.Millisecond)
		PoolTestProcessingUntilComplete(p, clientConnected, t)
		PoolWaiting(p, t)
		GetClientPoolConn(clientConnected, p).Conn.Close()
		time.Sleep(10 * time.Millisecond)
		PoolFree(p, t)
	}
}

func TestMineBarrage(t *testing.T) {
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p, t)

	/* no miners */
	SendMiningRequestAsync(t, c, "kazooom", 10000000)
	PoolContainsNMiners(p, 0, t)
	time.Sleep(10 * time.Millisecond)
	if len(c.PoolConns) == 0 {
		t.Errorf("Client did not remain connected with no miners: %v", c)
	}
	PoolWaiting(p, t)

	numRequests := 10
	for i := 0; i < numRequests; i++ {
		nonces, _ := c.Mine("pow pow", 1000000)
		if nonces[GetClientPoolConn(c, p).Conn.RemoteAddr()] != -1 {
			t.Error("Client was able to send multiple mine requests")
		}
	}
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p, t)

	///* miners */
	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	CreateTestingMiner(t, p)
	time.Sleep(1 * time.Millisecond)
	PoolContainsNMiners(p, 3, t)
	SendMiningRequestAsync(t, c, "kazooom", 100000)
	time.Sleep(1 * time.Millisecond)
	_, present := c.PoolConns[GetClientPoolConn(c, p).Conn.RemoteAddr()]
	if !present {
		t.Errorf("Client was not connected to pool: %v", c)
	}
	PoolInProcessingState(p, c, t)

	//for i := 0; i < numRequests; i++ {
	//	c.Mine("pow pow", 1000)
	//	time.Sleep(4 * time.Millisecond)
	//	_, present := c.PoolConns[GetClientPoolConn(c, p).Conn.RemoteAddr()]
	//	// BusyPool should disconnect client
	//	if present {
	//		t.Error("Client was able to send multiple mine requests")
	//	}
	//	PoolWaiting(p, t) // waiting now that cancelled
	//}

	PoolTestProcessingUntilComplete(p, c, t)
	PoolWaiting(p, t)
	p.Client.Conn.Close()
	time.Sleep(10 * time.Millisecond)
	PoolFree(p, t)
}

func TestManyPoolsPerClient(t *testing.T) {
	p1, _ := CreateTestingPool(t)
	PoolFree(p1, t)
	p2, _ := CreateTestingPool(t)
	PoolFree(p2, t)
	p3, _ := CreateTestingPool(t)
	PoolFree(p3, t)

	CreateTestingMiner(t, p1)
	CreateTestingMiner(t, p1)
	CreateTestingMiner(t, p1)
	CreateTestingMiner(t, p2)
	CreateTestingMiner(t, p2)
	CreateTestingMiner(t, p3)

	c := CreateClient([]string{p1.Addr.String(), p2.Addr.String(), p3.Addr.String()})
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p1, t)
	PoolWaiting(p2, t)
	PoolWaiting(p3, t)

	SendMiningRequestAsync(t, c, "kazooom", 10000000)
	time.Sleep(1 * time.Millisecond)
	PoolInProcessingState(p1, c, t)
	PoolInProcessingState(p2, c, t)
	PoolInProcessingState(p3, c, t)

	PoolTestProcessingUntilComplete(p3, c, t) // will take longest
	PoolWaiting(p1, t)
	PoolWaiting(p2, t)
	PoolWaiting(p3, t)
}
