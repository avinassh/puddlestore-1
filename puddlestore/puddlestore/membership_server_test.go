package puddlestore

import (
	//"fmt"
	//"strings"
	//"testing"
	//
	//"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/raft"
	"fmt"
	"strings"
	"testing"
)

func TestStartRaftCluster(t *testing.T) {
	cluster, _ := StartRaftCluster(DEFAULT_ZK_ADDR)

	// check that all nodes are visible to each
	conn, err := ZookeeperConnect(DEFAULT_ZK_ADDR)
	defer conn.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	for _, n := range cluster.Nodes {
		path := fmt.Sprintf("/raft/%v", n.Id)
		data, _, err := conn.Get(path)
		if err != nil {
			t.Errorf("%v", err)
		}
		info := strings.Split(string(data), ",")
		if info[1] != n.Id {
			t.Errorf("Node in Zookeeper has incorrect information")
		} else {
			fmt.Printf("found znode with ID: %v\n", n.Id)
		}
	}

}

func TestStartTapestryCluster(t *testing.T) {
	cluster, err := StartTapestryCluster(DEFAULT_ZK_ADDR, 5)

	if err != nil {
		t.Errorf("%v", err)
		return
	}

	// check that all nodes are visible to each
	conn, err := ZookeeperConnect(DEFAULT_ZK_ADDR)
	defer conn.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	for _, n := range cluster.Nodes {
		id := n.GetRemoteSelf().Id.String()
		path := fmt.Sprintf("/tapestry/%v", id)
		data, _, err := conn.Get(path)
		if err != nil {
			t.Errorf("%v", err)
			return
		}
		info := strings.Split(string(data), ",")
		if info[1] != id {
			t.Errorf("Node in Zookeeper has incorrect information")
		} else {
			fmt.Printf("found tapestry znode with ID: %v\n", id)
		}
	}

}
