package puddlestore

import (
	"fmt"
	"os"

	"io/ioutil"

	"math/rand"

	"time"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/raft"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_tapestry/tapestry"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc/grpclog"
)

/************************************************************************************/
// Testing methods for generating Raft/Tapestry nodes
/************************************************************************************/
type RaftCluster struct {
	Nodes []*raft.RaftNode
	Conns map[string]*zk.Conn // map giving access to connections (node.id => connection)
}

type TapestryCluster struct {
	Nodes []*tapestry.Node
	Conns map[string]*zk.Conn // map from node IDs to zookeeper connections
}

func newRaftCluster() *RaftCluster {
	var cluster RaftCluster
	cluster.Nodes = make([]*raft.RaftNode, 0)
	cluster.Conns = make(map[string]*zk.Conn)
	return &cluster
}

func newTapestryCluster() *TapestryCluster {
	var cluster TapestryCluster
	cluster.Nodes = make([]*tapestry.Node, 0)
	cluster.Conns = make(map[string]*zk.Conn)
	return &cluster
}

func SuppressLoggers(suppress bool) {
	if suppress {
		Out.SetOutput(ioutil.Discard)
		Error.SetOutput(ioutil.Discard)
		Debug.SetOutput(ioutil.Discard)
		raft.Out.SetOutput(ioutil.Discard)
		raft.Error.SetOutput(ioutil.Discard)
		raft.Debug.SetOutput(ioutil.Discard)
		tapestry.Out.SetOutput(ioutil.Discard)
		tapestry.Error.SetOutput(ioutil.Discard)
		tapestry.Debug.SetOutput(ioutil.Discard)
		grpclog.SetLogger(Out)
	}
}

// Starts all the nodes that are required for a PuddleStore cluster, assuming a Zookeeper server is active
// at "zkservers"
func StartPuddleStoreCluster(zkservers string, tapestrySize int) (*RaftCluster, *TapestryCluster, error) {
	raft.SetDebug(false)
	tapestry.SetDebug(false)

	rcluster, err := StartRaftCluster(zkservers)
	if err != nil {
		return nil, nil, err
	}
	tcluster, err := StartTapestryCluster(zkservers, tapestrySize)
	if err != nil {
		return nil, nil, err
	}
	time.Sleep(PuddleStoreRaftConfig().ElectionTimeout * 3)
	return rcluster, tcluster, nil
}

// StartRaftCluster starts a Raft cluster that is connected to Zookeeper
func StartRaftCluster(zkservers string) (*RaftCluster, error) {
	RemoveRaftLogs()
	num := PuddleStoreRaftConfig().ClusterSize
	cluster := newRaftCluster()

	// create all the children as ephemeral nodes
	for i := 0; i < num; i++ {
		rnode, conn, err := CreatePuddleStoreRaftNode(zkservers, 0, false)
		if err != nil {
			return nil, err
		}
		cluster.Conns[rnode.Id] = conn
		cluster.Nodes = append(cluster.Nodes, rnode)
	}

	return cluster, nil
}

// Starts TapestryCluster with n nodes that all interface with Zookeeper
func StartTapestryCluster(zkservers string, n int) (*TapestryCluster, error) {
	cluster := newTapestryCluster()

	// add nodes to the network one by one
	for i := 0; i < n; i++ {
		tnode, conn, err := CreatePuddleStoreTapestryNode(zkservers, 0, false)
		if err != nil {
			return nil, err
		}
		cluster.Nodes = append(cluster.Nodes, tnode)
		cluster.Conns[tnode.GetRemoteSelf().Id.String()] = conn
	}

	return cluster, nil
}

func (c *RaftCluster) StopAll() {
	for _, node := range c.Nodes {
		// stop the node
		node.GracefulExit()
		// close the connection
		c.Conns[node.Id].Close()
	}
}

func (c *TapestryCluster) StopAll() {
	for _, node := range c.Nodes {
		// stop the node
		node.Leave()
		// close the connection
		c.Conns[node.GetRemoteSelf().Id.String()].Close()
	}
}

func RemoveRaftLogs() {
	if rmErr := os.RemoveAll("raftlogs/"); rmErr != nil {
		fmt.Errorf("could not remove raftlogs; continuing: %v", rmErr)
	}
}

/************************************************************************************/
// Testing methods for generating Raft/Tapestry nodes
/************************************************************************************/

var RAND_NOUNS = [...]string{"time", "year", "people", "way", "day", "man", "thing", "woman", "life", "child", "world", "school", "state", "family", "student", "group", "country", "problem", "hand", "part", "place", "case", "week", "company", "system", "program", "question", "work", "government", "number", "night", "point", "home", "water", "room", "mother", "area", "money", "story", "fact", "month", "lot", "right", "study", "book", "eye", "job", "word", "business", "issue", "side", "kind", "head", "house", "service", "friend", "father", "power", "hour", "game", "line", "end", "member", "law", "car", "city", "community", "name", "president", "team", "minute", "idea", "kid", "body", "information", "back", "parent", "face", "others", "level", "office", "door", "health", "person", "art", "war", "history", "party", "result", "change", "morning", "reason", "research", "girl", "guy", "moment", "air", "teacher", "force", "education"}

func GetRandName(num int) string {
	str := RAND_NOUNS[rand.Intn(len(RAND_NOUNS))]
	for i := 1; i < num; i++ {
		str += "-" + RAND_NOUNS[rand.Intn(len(RAND_NOUNS))]
	}
	return str
}

func GetRandData(num int) string {
	str := RAND_NOUNS[rand.Intn(len(RAND_NOUNS))]
	for i := 1; i < num; i++ {
		str += " " + RAND_NOUNS[rand.Intn(len(RAND_NOUNS))]
	}
	return str
}

func GetRandBytes(size int) []byte {
	bts := make([]byte, 0)
	for i := 0; i < size; i++ {
		bts = append(bts, byte(rand.Intn(256)))
	}
	return bts
}

func WithProb(prob float64) bool {
	return rand.Float64() <= prob
}
