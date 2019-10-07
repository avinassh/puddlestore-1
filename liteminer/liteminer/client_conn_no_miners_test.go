package liteminer

import (
	"testing"
	"time"
)

/*
Tests whether a client that connects when no miners are added
receives progress when miners are added
*/
func TestWaitOnNoMiners(t *testing.T) {

	Out.Printf("")
	Out.Printf("Starting TestWaitOnNoMiners...\n")
	Out.Printf("")

	p, _ := CreateTestingPool(t)
	c := CreateTestingClient(p)

	// no miners connected
	PoolContainsNMiners(p, 0, t)

	// mining
	c.Mine("test1", 10000)

	// pool should be waiting since it has no miners
	PoolWaiting(p, t)
	CreateTestingMiner(t, p)
	PoolWaiting(p, t)
	CreateTestingMiner(t, p)
	PoolWaiting(p, t)
	CreateTestingMiner(t, p)
	PoolWaiting(p, t)
	CreateTestingMiner(t, p)
	time.Sleep(time.Millisecond)
	// miners added
	PoolContainsNMiners(p, 4, t)
	PoolWaiting(p, t)

	// client sends another mine request
	c.Mine("test2", 10000)
	PoolTestProcessingUntilComplete(p, c, t)
	c.Disconnect(p.Client.Conn.LocalAddr())
	// allows time for message to reach
	time.Sleep(time.Millisecond * 50)
	PoolFree(p, t)

	// tests that it can now receive another connection
	c = CreateTestingClient(p)
	time.Sleep(time.Millisecond * 10)
	PoolWaiting(p, t)
	c.Mine("test3", 10000)
	// allow time to connect
	time.Sleep(time.Millisecond * 10)
	PoolTestProcessingUntilComplete(p, c, t)
	PoolWaiting(p, t)
	c.Disconnect(p.Client.Conn.LocalAddr())
	time.Sleep(time.Millisecond*10)
	PoolFree(p, t)

}
