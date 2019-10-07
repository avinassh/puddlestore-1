package puddlestore

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"os"
	"os/signal"
	"syscall"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/keyvaluestore"
	raftClient "github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/client"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/raft"
	tapestryClient "github.com/brown-csci1380/s18-mcisler-vmathur2/ta_tapestry/client"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_tapestry/tapestry"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

/*
	Helper methods to interface with the membership server to store information
	on system and sub-cluster configuration.
	Essentially the interface between PuddleStore and Zookeeper, as well as between Raft/Tapestry nodes and Zookeeper.
*/

const TAPESTRY_CLUSTER_ROOT = "/tapestry"
const RAFT_CLUSTER_ROOT = "/raft"
const RAFT_ROOT_DATA = "raftNodes"
const TAPESTRY_ROOT_DATA = "tapestryNodes"
const NAME_OF_ROOT_DIR = "root"
const FIRST_RAFT_NODE_ROOT = "/first_raft_node"
const MAX_RAFT_STARTUP_RETRIES = 5
const RAFT_STARTUP_RETRY_WAIT = 10 * time.Millisecond

/************************************************************************************/
// Interface for Client INode interactions
/************************************************************************************/

/*
	ZookeeperConnect connects to the given Zookeeper servers and returns its conn object or an error on error.
*/
func ZookeeperConnect(zkservers string) (*zk.Conn, error) {
	zks := strings.Split(zkservers, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	return conn, err
}

/*
	Connects to Zookeeper and retrieves the data on the root node, and returns that Inode (a directory).
*/
func ZookeeperGetRoot(conn *zk.Conn) (inode *INode, err error) {
	// check if the Zookeeper system is set up
	exists, _, err := conn.Exists("/root")
	if err != nil {
		return nil, fmt.Errorf("finding root: %v", err.Error())
	}

	if !exists {
		// if it doesn't exist, create it as the first one and add it to Zookeeper
		inode, err := createInode(conn, NAME_OF_ROOT_DIR, DIRECTORY)
		if err != nil {
			return nil, fmt.Errorf("creating root: %v", err)
		}
		_, err = conn.Create("/root", []byte(inode.AGUID), int32(0), zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, fmt.Errorf("creating root: %v", err)
		}
		return inode, nil

	} else {
		// if it does exist, look up the associated inode and return it
		aguid_bytes, _, err := conn.Get("/root")
		if err != nil {
			return nil, fmt.Errorf("finding root: %v", err.Error())
		}
		// go and get the inode referenced in Zookeeper, to return
		bytes, err := fetchByAGUID(conn, string(aguid_bytes))
		if err != nil {
			return nil, fmt.Errorf("finding root: %v", err.Error())
		}
		inode, err := decodeInode(bytes, conn)
		if err != nil {
			return nil, fmt.Errorf("finding root: %v", err.Error())
		}
		return inode, nil
	}
}

/*
	Determines based on the Zookeeper state if PuddleStore is ready to manage files.
	I.e. returns whether there is a complete Raft cluster up and at least MIN_TAPESTRY_NODES Tapestry node up.
	Note that the presence of a root inode doesn't matter.
*/
func PuddleStoreReady(conn *zk.Conn) (ready bool, err error) {
	raftConfig := PuddleStoreRaftConfig()
	// check for existence of roots first
	raftExists, _, err := conn.Exists(RAFT_CLUSTER_ROOT)
	if err != nil {
		return false, err
	}
	if !raftExists {
		return false, errors.New("no Raft consistency cluster present")
	}

	tapestryExists, _, err := conn.Exists(TAPESTRY_CLUSTER_ROOT)
	if err != nil {
		return false, err
	}
	if !tapestryExists {
		return false, errors.New("no Tapestry object store cluster present")
	}

	// then check if there are a sufficient number of nodes in each
	raftNodes, _, err := conn.Children(RAFT_CLUSTER_ROOT)
	if err != nil {
		return false, err
	}
	if len(raftNodes) < raftConfig.ClusterSize {
		return false, errors.New("insufficient number of nodes present in Raft consistency cluster")
	}

	tapestryNodes, _, err := conn.Children(TAPESTRY_CLUSTER_ROOT)
	if err != nil {
		return false, err
	}
	if len(tapestryNodes) < MIN_TAPESTRY_NODES {
		return false, errors.New("insufficient number of nodes present in Tapestry object store cluster")
	}

	return true, nil
}

/*
	Clears any filesystem state from the Zookeeper store (i.e. root node, etc.).
	Should be called whenever there are insufficient resources in the cluster to have
	a persistent storage of the filesystem.
	In our case, this is whenever there are no more Raft or Tapestry nodes in the cluster.
	Returns any error.
*/
func clearFilesystemState(conn *zk.Conn) error {
	// check for existence of roots first
	return conn.Delete("/root", 0)
}

/************************************************************************************/
// Interface for Raft/Tapestry nodes
/************************************************************************************/

/*
	RAFT
*/

/*
	CreatePuddleStoreRaftNode creates a RaftNode using the raft library with the given port,
	connecting to the current Raft cluster stored in Zookeeper or creating a new one if it's the first node created.
	Returns the new RaftNode, the Zookeeper connection, and any error.
	NOTE the Zookeeper connection representing the ephemeral node MUST be closed when the program closes
*/
func CreatePuddleStoreRaftNode(zkservers string, port int, debug bool) (raftNode *raft.RaftNode, econn *zk.Conn, err error) {
	raft.SetDebug(debug)

	// connect to Zookeeper to find nodes
	econn, err = ZookeeperConnect(zkservers)
	if err != nil {
		return nil, nil, err
	}

	// create the Raft directory in Zookeeper if it doesn't exist
	err = zkCreateClusterRoot(zkservers, RAFT_CLUSTER_ROOT, RAFT_ROOT_DATA)
	if err != nil {
		econn.Close()
		return nil, nil, err
	}

	// try and create the special "first raft (ephemeral) node". The race to create
	// this node will determine the node that everyone else will connect to.
	// (note we race to create a blank node first, and then the first one to do so will
	// proceed on to create the raftNode while the others wait (retrying), and then the first
	// one, once created, will come back and update this first node with its RemoteNode,
	// so other can connect to it)
	weCreated, firstNode, err := raceToBeFirstRaftNode(econn)
	if err != nil {
		econn.Close()
		return nil, nil, err
	}

	// if we created the first node, we should start with a nil remoteNode so
	// others will connect to us
	if weCreated {
		// since we're the first, we have the responsibility to clean up the filesystem
		// so that a new one will be created that doesn't reference old state
		clearFilesystemState(econn)

		// then, start up a standalone node to wait for other connections
		raftNode, err = createAndAddRaftNode(econn, port, nil)
		if err != nil {
			econn.Close()
			return nil, nil, err
		}
		// and finally, update our original special "first node" with our address
		// so other waiting nodes can finally try and connect to us
		err = updateFirstRaftNode(econn, raftNode.GetRemoteSelf())
		if err != nil {
			econn.Close()
			return nil, nil, err
		}
		return raftNode, econn, nil

	} else {
		raftNode, err = createAndAddRaftNode(econn, port, firstNode)
		return
	}
}

// tries to create the special "first raft node" object, and if it fails, continually re-queries
// the existing "first raft node" until it can find the address of the first, which it returns.
// (this is accompanied by the updateFirstRaftNode function, called by the first node after creating
// the blank first node AND finishing starting up such that it can post its address)
func raceToBeFirstRaftNode(econn *zk.Conn) (weCreated bool, firstNode *raft.RemoteNode, err error) {
	exists, _, err := econn.Exists(FIRST_RAFT_NODE_ROOT)
	if err != nil {
		return false, nil, err
	}
	// if it doesn't exist, we've won the race and can create it
	// (it must be ephemeral in case the first node dies or we start a new cluster)
	if !exists {
		acl := zk.WorldACL(zk.PermAll)
		_, err = econn.Create(FIRST_RAFT_NODE_ROOT, []byte{}, int32(zk.FlagEphemeral), acl)
		if err != nil {
			return false, nil, err
		}
		return true, nil, nil

	} else {
		// otherwise, we'll need to wait until the actual first node finishes starting
		// and sets the special first node file to be its address
		for try := 0; try < MAX_RAFT_STARTUP_RETRIES; try++ {
			data, _, err2 := econn.Get(FIRST_RAFT_NODE_ROOT)
			if err2 != nil {
				err = err2
				// but keep trying...
			} else {
				addr, id, err := zkDeserializeNode(data)
				if err == nil {
					// if we successfully parsed, return the data
					return false, &raft.RemoteNode{addr, id}, nil
				}
				// if err is non-nil, it means there was no data or invalid data there, so try again
			}
			<-time.NewTimer(RAFT_STARTUP_RETRY_WAIT).C
		}
		return false, nil, errors.New("attempt to find existing raft node timeout out")
	}
}
func updateFirstRaftNode(econn *zk.Conn, remoteNode *raft.RemoteNode) error {
	_, err := econn.Set(FIRST_RAFT_NODE_ROOT, zkSerializeNode(remoteNode.Addr, remoteNode.Id), 0)
	return err
}

func createAndAddRaftNode(econn *zk.Conn, port int, remoteNode *raft.RemoteNode) (raftNode *raft.RaftNode, err error) {
	// Initialize Raft with PuddleStore config
	config := PuddleStoreRaftConfig()

	// Create Raft node
	rnode, err := raft.CreateNode(port, remoteNode, new(keyvaluestore.KeyValueStore), config)
	if err != nil {
		return nil, err
	}

	// Connect to Zookeeper and add raft node information
	err = zkAddClusterNode(econn, rnode.GetRemoteSelf().GetAddr(), rnode.GetRemoteSelf().GetId(), RAFT_CLUSTER_ROOT)
	if err != nil {
		return nil, err
	}
	// make sure closed on shutdown signal
	registerOnCloseSignalHandler(econn)
	return rnode, err
}

/*
	Returns a RaftClient connected to a random RaftNode in Zookeeper
	If no Raft nodes are found, throws an error stating "no nodes found"
*/
func zkGetRaftClient(conn *zk.Conn) (*raftClient.Client, error) {
	children, _, err := conn.Children(RAFT_CLUSTER_ROOT)
	if err != nil {
		return nil, err
	}
	for _, i := range randomIndices(len(children)) {
		addr, _, found, err2 := zkGetClusterNode(conn, RAFT_CLUSTER_ROOT, i)
		if !found || err2 != nil {
			err = err2
			continue
		}
		// now, try and connect and confirm we can. If we can't, try another node
		client, err2 := raftClient.Connect(addr)
		if err2 != nil {
			err = err2
			continue
		}
		return client, nil
	}
	return nil, errors.New("no reachable Raft nodes found")
}

/*
	TAPESTRY
*/
/*
	CreatePuddleStoreTapestryNode creates a Tapestry Node using the tapestry library with the given port,
	connecting to the current tapestry cluster stored in Zookeeper or creating a new one if it's the first node created.
	Returns the new Node, the Zookeeper connection, and any error.
	NOTE the Zookeeper connection representing the ephemeral node MUST be closed when the program closes
*/
func CreatePuddleStoreTapestryNode(zkservers string, port int, debug bool) (tapestryNode *tapestry.Node, econn *zk.Conn, err error) {
	tapestry.SetDebug(debug)

	// connect to Zookeeper to find nodes
	econn, err = ZookeeperConnect(zkservers)
	if err != nil {
		return nil, nil, err
	}

	// create the tapestry directory in Zookeeper if it doesn't exist
	err = zkCreateClusterRoot(zkservers, TAPESTRY_CLUSTER_ROOT, TAPESTRY_ROOT_DATA)
	if err != nil {
		econn.Close()
		return nil, nil, err
	}

	// look up any tapestry nodes known to Zookeeper and get one's address
	addr, _, found, err := zkGetClusterNode(econn, TAPESTRY_CLUSTER_ROOT, -1)
	if err != nil {
		econn.Close()
		return nil, nil, err
	}
	connectTo := ""
	if found {
		connectTo = addr
	} else {
		// if none were found, we have the responsibility to clean up the filesystem
		// so that a new one will be created that doesn't reference old state
		clearFilesystemState(econn)
	}
	// if none were found, connectTO will be "", and Tapestry will create it as the first
	// and add it to Zookeeper for future nodes to connect to

	// Create Tapestry node
	tnode, err := tapestry.Start(port, connectTo)
	if err != nil {
		econn.Close()
		return nil, nil, err
	}

	// Connect to Zookeeper and add raft node information
	err = zkAddClusterNode(econn, tnode.GetRemoteSelf().Address, tnode.GetRemoteSelf().Id.String(), TAPESTRY_CLUSTER_ROOT)
	if err != nil {
		econn.Close()
		return nil, nil, err
	}
	// make sure closed on shutdown signal
	registerOnCloseSignalHandler(econn)
	return tnode, econn, err
}

/*
	Returns a TapestryClient connected to a random Tapestry node in Zookeeper
	If no Tapestry nodes are found, throws an error stating "no nodes found"
*/
func zkGetTapestryClient(conn *zk.Conn) (client *tapestryClient.Client, err error) {
	children, _, err := conn.Children(TAPESTRY_CLUSTER_ROOT)
	if err != nil {
		return nil, err
	}
	for _, i := range randomIndices(len(children)) {
		addr, _, found, err2 := zkGetClusterNode(conn, TAPESTRY_CLUSTER_ROOT, i)
		if !found || err2 != nil {
			err = err2
			continue
		}
		// now, try and connect and confirm we can. If we can't, try another node
		client, err2 := tapestryClient.ConnectAndCheck(addr)
		if err2 != nil {
			err = err2
			continue
		}
		return client, nil
	}
	return nil, errors.New("no reachable Raft nodes found")
}

/*
	Common Helpers, based on the consistent storage of nodes under "/<cluster>" as "<addr>,<id>"
*/

// Registers a signal handler for SIGINT and SIGTERM to close the zookeeper connection
// (most important for ephemeral nodes so they don't have to timeout)
func registerOnCloseSignalHandler(conn *zk.Conn) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// wait on any SIGINT or SIGTERM signals and close the conn on receipt
	go func() {
		<-sigs
		conn.Close()
		fmt.Print("exited")
		os.Exit(0) // need to exit because we masked the signal
	}()
}

// zookeeperCreateRaftRoot creates the root directory (non-ephemeral) of all the raft nodes in Zookeeper
func zkCreateClusterRoot(zkservers, clusterPrefix, clusterData string) error {
	acl := zk.WorldACL(zk.PermAll)
	conn, err := ZookeeperConnect(zkservers)
	if err != nil {
		return err
	}
	exists, _, err := conn.Exists(clusterPrefix)
	if err != nil {
		return err
	}
	if !exists {
		_, err = conn.Create(clusterPrefix, []byte(clusterData), int32(0), acl)
		return err
	}
	return nil // already exists
}

// zookeeperAddRaftNode adds the given raft node to the Zookeeper listing
func zkAddClusterNode(conn *zk.Conn, addr, id, clusterPrefix string) (err error) {
	acl := zk.WorldACL(zk.PermAll)
	path := fmt.Sprintf("%s/%v", clusterPrefix, id)
	_, err = conn.Create(path, zkSerializeNode(addr, id), int32(zk.FlagEphemeral), acl)
	return err
}

/*
	Returns the addr and id of either a raft or tapestry node found in the membership server listing
	at pathPrefix, or nothing if none found. Gets the "index" child if index is positive. If negative,
	gets a random child.
*/
func zkGetClusterNode(conn *zk.Conn, clusterPrefix string, index int) (addr string, id string, found bool, err error) {
	children, _, err := conn.Children(clusterPrefix)
	if err != nil {
		return "", "", false, err
	}

	if len(children) > 0 && index < len(children) {
		path := clusterPrefix + "/"
		if index < 0 {
			path += children[rand.Intn(len(children))]
		} else {
			path += children[index]
		}

		data, _, err := conn.Get(path)
		if err != nil {
			return "", "", false, err
		}
		// try and deserialize the data there
		addr, id, err = zkDeserializeNode(data)
		return addr, id, true, err
	}
	return "", "", false, nil
}

// Returns the data stored under a cluster node in Zookeeper
func zkSerializeNode(addr, id string) []byte {
	return []byte(fmt.Sprintf("%v,%v", addr, id))
}

// Returns the fields stored in the given data from a cluster node, or an error if the data is invalid
func zkDeserializeNode(data []byte) (addr, id string, err error) {
	fields := strings.Split(string(data), ",")
	if len(fields) != 2 {
		return "", "", errors.New("invalid data stored under node")
	}
	return fields[0], fields[1], nil
}

/*
	Generates a list of shuffled indicies out of an original list of [1...n],
	by shuffling them and returning the new shuffled list.
	Uses the Fisher–Yates shuffle method.
*/
func randomIndices(n int) []int {
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}
	for i := 0; i < n-2; i++ {
		exchangeI := rand.Intn(n-i) + i
		tmp := indices[i]
		indices[i] = indices[exchangeI]
		indices[exchangeI] = tmp
	}
	return indices
}
