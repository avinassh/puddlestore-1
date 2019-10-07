package liteminer

import (
	"testing"
	"time"
)

// checks that all disconnections are handled gracefully

func TestMinerDisconnectPool(t *testing.T) {

	Out.Printf("")
	Out.Printf("Starting TestMinerDisconnectPool...")
	Out.Printf("")

	/* not mining */
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	m := CreateTestingMiner(t, p)
	time.Sleep(10 * time.Millisecond)

	// bam: shutdown
	m.conn.Conn.Close()
	m.Shutdown() // so it doesn't go crazy

	PoolFree(p, t) // mainly to test if pool still alive, not if it's free

	/* mining */
	p, _ = CreateTestingPool(t)
	PoolFree(p, t)
	m = CreateTestingMiner(t, p)
	_ = CreateTestingMiner(t, p) // this one will finish
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 2, t)
	PoolWaiting(p, t)
	SendMiningRequestAsync(t, c, "lolz", 100000)
	time.Sleep(1 * time.Millisecond)
	PoolInProcessingState(p, c, t)

	// bam: shutdown
	m.conn.Conn.Close()
	m.Shutdown() // so it doesn't go crazy

	time.Sleep(1 * time.Millisecond)
	PoolContainsNMiners(p, 1, t)
	PoolInProcessingState(p, c, t)
	PoolTestProcessingUntilComplete(p, c, t)
	PoolWaiting(p, t)
}

// check tolerance if miner drops by timing out (doesn't send heartbeat)
func TestPoolDisconnectMiner(t *testing.T) {

	Out.Printf("")
	Out.Printf("Starting TestPoolDisconnectMiner...\n")
	Out.Printf("")

	/* not mining */
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	m := CreateTestingMiner(t, p)
	time.Sleep(10 * time.Millisecond)

	// bam: shutdown
	GetPoolMinerConn(p, m).Conn.Close()

	PoolFree(p, t) // mainly to test if pool still alive, not if it's free

	/* mining */
	p, _ = CreateTestingPool(t)
	PoolFree(p, t)
	m = CreateTestingMiner(t, p)
	_ = CreateTestingMiner(t, p) // this one will finish
	time.Sleep(1 * time.Millisecond)
	PoolContainsNMiners(p, 2, t)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p, t)
	SendMiningRequestAsync(t, c, "lolz2", 100000)
	time.Sleep(1 * time.Millisecond)
	PoolInProcessingState(p, c, t)

	// bam: shutdown
	GetPoolMinerConn(p, m).Conn.Close()

	time.Sleep(1 * time.Millisecond)
	PoolContainsNMiners(p, 1, t)
	PoolInProcessingState(p, c, t)
	PoolTestProcessingUntilComplete(p, c, t)
	PoolWaiting(p, t)
	PoolContainsNMiners(p, 1, t)
}

func TestPoolKickMiner(t *testing.T) {

	Out.Printf("")
	Out.Printf("Starting TestPoolKickMiner...\n")
	Out.Printf("")

	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	m := CreateTestingMiner(t, p)

	// if you stop beating but you aren't mining, you don't get kicked
	m.StopBeating()
	time.Sleep(time.Second*4)
	PoolContainsNMiners(p, 1, t)

	// if you connect to a client and start mining, you do get kicked
	PoolFree(p, t)
	c := CreateTestingClient(p)
	SendMiningRequestAsync(t, c, "test", 10000000)
	time.Sleep(time.Second*4)
	PoolContainsNMiners(p, 0, t)
}

func TestClientDisconnectPool(t *testing.T) {

	Out.Printf("")
	Out.Printf("Starting TestClientDisconnectPool...\n")
	Out.Printf("")

	/* not mining */
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p, t)

	// bang: disconnect
	GetClientPoolConn(c, p).Conn.Close()

	time.Sleep(10 * time.Millisecond)
	PoolFree(p, t)

	/* mining */
	p, _ = CreateTestingPool(t)
	PoolFree(p, t)
	_ = CreateTestingMiner(t, p)
	c = CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 1, t)
	PoolWaiting(p, t)
	SendMiningRequestAsync(t, c, "lolz2", 10000)
	time.Sleep(1 * time.Millisecond)
	PoolInProcessingState(p, c, t)
	time.Sleep(5 * time.Millisecond) // give it a little buffer

	// bang: disconnect
	GetClientPoolConn(c, p).Conn.Close()

	PoolTestProcessingUntilCompleteClientless(p, c, t)
	PoolFree(p, t)
	PoolContainsNMiners(p, 1, t)
}

func TestPoolDisconnectClient(t *testing.T) {
	Out.Printf("")
	Out.Printf("Starting TestPoolDisconnectClient...\n")
	Out.Printf("")

	/* not mining */
	p, _ := CreateTestingPool(t)
	PoolFree(p, t)
	c := CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolWaiting(p, t)

	// bong: disconnect
	p.Client.Conn.Close()

	time.Sleep(10 * time.Millisecond)
	PoolFree(p, t)

	/* mining */
	p, _ = CreateTestingPool(t)
	PoolFree(p, t)
	_ = CreateTestingMiner(t, p)
	c = CreateTestingClient(p)
	time.Sleep(10 * time.Millisecond)
	PoolContainsNMiners(p, 1, t)
	PoolWaiting(p, t)
	SendMiningRequestAsync(t, c, "lolz2", 10000)
	time.Sleep(1 * time.Millisecond)
	PoolInProcessingState(p, c, t)
	time.Sleep(5 * time.Millisecond) // give it a little buffer

	// bang: disconnect
	p.Client.Conn.Close()

	PoolTestProcessingUntilCompleteClientless(p, c, t)
	PoolFree(p, t)
	PoolContainsNMiners(p, 1, t)
}