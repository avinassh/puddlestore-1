package puddlestore

import (
	"math/rand"
	"testing"

	tapestryClient "github.com/brown-csci1380/s18-mcisler-vmathur2/ta_tapestry/client"
)

func TestTapestry(t *testing.T) {

	numClients := 5
	clients := make([]*tapestryClient.Client, numClients)

	for i := 0; i < numClients; i++ {
		conn, err := ZookeeperConnect(DEFAULT_ZK_ADDR)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		c, err := zkGetTapestryClient(conn)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		clients[i] = c
	}

	numRequests := 20
	requests := make(map[string]string)

	for i := 0; i < numRequests; i++ {
		id := newGUID()
		data := newGUID()
		requests[id] = data
		c := clients[rand.Intn(numClients)]
		c.Store(id, []byte(data))
	}

	for key, value := range requests {
		t.Logf("Looking up reqest %v", key)
		c := clients[rand.Intn(numClients)]
		data, err := c.Get(key)
		if err != nil {
			t.Errorf(err.Error())
		}
		if string(data) != value {
			t.Errorf("values do not match")
		}
	}
}
