package raft

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	"math/rand"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/cs138"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/raft/keyvaluestore"
)

// NodeState represents one of four possible states a Raft node can be in.
type NodeState int

const (
	FOLLOWER_STATE NodeState = iota
	CANDIDATE_STATE
	LEADER_STATE
	JOIN_STATE
)

// StateMachine is a general interface defining the methods a Raft state machine
// should implement. For this project, we use a HashMachine as our state machine.
type StateMachine interface {
	GetState() (state interface{}) // Useful for testing once you use type assertions to convert the state
	ApplyCommand(command uint64, data []byte) (message string, err error)
	FormatCommand(command uint64) (commandString string)
}

// RaftNode defines an individual Raft node.
type RaftNode struct {
	Id         string
	State      NodeState
	Leader     *RemoteNode
	config     *Config
	nodeMutex  sync.Mutex
	IsShutdown bool

	port          int
	server        *grpc.Server
	NetworkPolicy *NetworkPolicy

	// Raft log cache (written to disk, do not use directly)
	logCache  LogCache
	raftLogFd FileData
	logMutex  sync.Mutex // STUDENT ADDED - MUTEX FOR WORKING WITH LOGCACHE -- DEPRECATED

	// Stable state (written to disk, use helper methods)
	stableState StableState
	ssMutex     sync.Mutex
	raftMetaFd  FileData

	// Leader specific volatile state
	commitIndex uint64            // highest log entry known to be committed
	lastApplied uint64            // we don't use this at all since commitIndex represents both states
	leaderMutex sync.Mutex        // use to control access to all leader specific state + logs because these are related
	nextIndex   map[string]uint64 // index after the element we currently know we are matched until (temporary)
	matchIndex  map[string]uint64 // index of element we are matched until (monotonically increasing)

	// Channels to send / receive various RPC messages (used in state functions)
	appendEntries  chan AppendEntriesMsg
	requestVote    chan RequestVoteMsg
	registerClient chan RegisterClientMsg
	clientRequest  chan ClientRequestMsg
	gracefulExit   chan bool

	// Replicated state machine (e.g. hash machine, kv-store etc.)
	stateMachine StateMachine

	// Client request map (used to store channels to respond through once a
	// request has been processed)
	requestsByCacheId       map[string]chan ClientReply
	registrationsByClientID map[uint64]chan RegisterClientReply // Stores channels to respond to RegisterClient once processed
	requestsMutex           sync.Mutex

	// debugging fields
	debugChan  chan NodeState //only initialized in the cluster
	debugMutex sync.Mutex
	debugCond  *sync.Cond
}

// CreateNode creates a new Raft node at the specified port, with the specified
// config, and connects to the provided remote node. Returns a pointer to the
// newly created Raft node.
func CreateNode(localPort int, remoteAddr *RemoteNode, config *Config) (rp *RaftNode, err error) {
	var r RaftNode
	rp = &r

	r.config = config
	r.IsShutdown = false

	var conn net.Listener
	// Initialize network policy
	r.NetworkPolicy = NewNetworkPolicy()
	r.NetworkPolicy.PauseWorld(false)

	// Initialize leader specific state
	r.commitIndex = 0
	r.lastApplied = 0
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)

	// Initialize RPC channels
	r.appendEntries = make(chan AppendEntriesMsg)
	r.requestVote = make(chan RequestVoteMsg)
	r.registerClient = make(chan RegisterClientMsg)
	r.clientRequest = make(chan ClientRequestMsg)
	r.gracefulExit = make(chan bool)

	// Initialize state machine (in Puddlestore, you'll switch this with your
	// own state machine)
	r.stateMachine = new(keyvaluestore.KeyValueStore)

	// Initialize client request cache
	r.requestsByCacheId = make(map[string]chan ClientReply)
	r.registrationsByClientID = make(map[uint64]chan RegisterClientReply)

	// Initialize debugging values
	r.debugCond = nil
	r.debugChan = nil

	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// Open listener on port...
	switch {
	case localPort != 0 && remoteAddr != nil:
		conn, err = OpenPort(localPort)
		if err != nil {
			return
		}
	case localPort != 0:
		conn, err = OpenPort(localPort)
		if err != nil {
			return
		}
	case remoteAddr != nil:
		conn, localPort, err = cs138.OpenListener()
		if err != nil {
			return
		}
	default:
		conn, localPort, err = cs138.OpenListener()
		if err != nil {
			return
		}
	}

	// Create node ID based on listener address
	r.Id = AddrToId(conn.Addr().String(), config.NodeIdSize)

	r.port = localPort
	Out.Printf("Started node with id:%v, listening at %v\n", r.Id, conn.Addr().String())

	// Initialize stable store
	freshNode, err := r.initStableStore()
	if err != nil {
		Error.Printf("Error intitializing the stable store: %v", err)
		return nil, err
	}

	r.setRemoteSelf(&RemoteNode{Id: r.Id, Addr: conn.Addr().String()})

	// Start RPC server
	serverOptions := []grpc.ServerOption{}
	r.server = grpc.NewServer(serverOptions...)
	RegisterRaftRPCServer(r.server, &r)
	go r.server.Serve(conn)

	if freshNode {
		// If current node is being newly created (as opposed to being restored
		// from stable state on disk):
		// - Connect to remote node if provided and join the cluster
		// - Else, start a cluster and wait for other nodes to join
		r.State = JOIN_STATE
		if remoteAddr != nil {
			err = remoteAddr.JoinRPC(&r)
		} else {
			Out.Printf("Waiting to start cluster until all have joined\n")
			go r.startCluster()
		}
	} else {
		// If current node is being restored from stable state on disk, start
		// running it in the follower state.
		r.State = FOLLOWER_STATE
		go r.run()
	}

	return
}

func (r *RaftNode) SetDebugChannel(db chan NodeState) {
	r.debugChan = db
}

// startCluster puts the current Raft node on hold until the required number of
// peers join the cluster. Once they do, it starts the peers via a StartNodeRPC
// call, and then starts the current node in the follower state.
func (r *RaftNode) startCluster() {
	r.nodeMutex.Lock()
	r.AppendToNodeList(*r.GetRemoteSelf())
	r.nodeMutex.Unlock()

	// Wait for all nodes to join cluster...
	for len(r.GetNodeList()) < r.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	// Start other nodes
	for _, node := range r.GetNodeList() {
		if r.Id != node.Id {
			Out.Printf("(%v) Starting node-%v\n", r.Id, node.Id)
			err := node.StartNodeRPC(r, r.GetNodeList())
			if err != nil {
				Error.Printf("Unable to start node: %v", err)
			}
		}
	}

	// Start the current Raft node, initially in follower state
	go r.run()
}

// stateFunction is a function defined on a Raft node, that while executing,
// handles the logic of the current state. When the time comes to transition to
// another state, the function returns the next function to execute.
type stateFunction func() stateFunction

func (r *RaftNode) run() {
	var curr stateFunction = r.doFollower
	for curr != nil {
		curr = curr()
	}
	// for testing
	r.IsShutdown = true
}

// Join adds the fromNode to the current Raft cluster.
func (r *RaftNode) Join(fromNode *RemoteNode) error {
	r.nodeMutex.Lock()
	defer r.nodeMutex.Unlock()

	if len(r.GetNodeList()) == r.config.ClusterSize {
		for _, node := range r.GetNodeList() {
			if node.Id == fromNode.Id {
				node.StartNodeRPC(r, r.GetNodeList())
				return nil
			}
		}

		r.Error("Warning! Unrecognized node tried to join after all other nodes have joined.\n")
		return fmt.Errorf("all nodes have already joined this Raft cluster")
	}

	r.AppendToNodeList(*fromNode)
	return nil
}

// StartNode is invoked on us by a remote node, and starts the current node in follower state.
func (r *RaftNode) StartNode(req *StartNodeRequest) error {
	r.nodeMutex.Lock()
	defer r.nodeMutex.Unlock()

	r.SetNodeList(req.NodeList)
	Out.Println(r.FormatNodeListIds("StartNode"))

	// Start the current Raft node, initially in follower state
	go r.run()

	return nil
}

type AppendEntriesMsg struct {
	request *AppendEntriesRequest
	reply   chan AppendEntriesReply
}

// AppendEntries is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (r *RaftNode) AppendEntries(req *AppendEntriesRequest) (AppendEntriesReply, error) {
	//r.Debug("AppendEntries request received from %v\n", req.Leader)
	reply := make(chan AppendEntriesReply)
	r.appendEntries <- AppendEntriesMsg{req, reply}
	return <-reply, nil
}

type RequestVoteMsg struct {
	request *RequestVoteRequest
	reply   chan RequestVoteReply
}

// RequestVote is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (r *RaftNode) RequestVote(req *RequestVoteRequest) (RequestVoteReply, error) {
	r.Debug("RequestVote request received from %v\n", req.Candidate)
	reply := make(chan RequestVoteReply)
	r.requestVote <- RequestVoteMsg{req, reply}
	return <-reply, nil
}

type RegisterClientMsg struct {
	request *RegisterClientRequest
	reply   chan RegisterClientReply
}

// RegisterClient is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding RegisterClientReply.
func (r *RaftNode) RegisterClient(req *RegisterClientRequest) (RegisterClientReply, error) {
	reply := make(chan RegisterClientReply)

	// If cluster hasn't started yet, return
	if r.State == JOIN_STATE {
		return RegisterClientReply{
			Status:     ClientStatus_CLUSTER_NOT_STARTED,
			ClientId:   0,
			LeaderHint: nil,
		}, nil
	}

	// Send request down channel to be processed by current stateFunction
	r.registerClient <- RegisterClientMsg{req, reply}
	return <-reply, nil
}

type ClientRequestMsg struct {
	request *ClientRequest
	reply   chan ClientReply
}

// ClientRequest is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding ClientReply.
func (r *RaftNode) ClientRequest(req *ClientRequest) (ClientReply, error) {
	r.Debug("ClientRequest request received\n")

	// If cluster hasn't started yet, return
	if r.State == JOIN_STATE {
		return ClientReply{
			Status:     ClientStatus_CLUSTER_NOT_STARTED,
			Response:   "",
			LeaderHint: nil,
		}, nil
	}

	reply := make(chan ClientReply)
	cr, exists := r.GetCachedReply(*req)

	if exists {
		// If the request has been cached, reply with existing response
		return *cr, nil
	}

	// Else, send request down channel to be processed by current stateFunction
	r.clientRequest <- ClientRequestMsg{req, reply}
	return <-reply, nil
}

// Exit abruptly shuts down the current node's process, including the GRPC server.
func (r *RaftNode) Exit() {
	Out.Printf("Abruptly shutting down node!")
	os.Exit(0)
}

// gracefulExit sends a signal down the gracefulExit channel, in order to enable
// a safe exit from the cluster, handled by the current stateFunction.
func (r *RaftNode) GracefulExit() {
	r.NetworkPolicy.PauseWorld(true)
	Out.Printf("%v Gracefully shutting down node!", r.Id)
	r.IsShutdown = true

	if r.State != JOIN_STATE {
		r.gracefulExit <- true
	}

	r.server.GracefulStop()
}


// Gets the current state from the StateMachine
func (r *RaftNode) GetStateMachineState() interface{} {
	return r.stateMachine.GetState()
}
