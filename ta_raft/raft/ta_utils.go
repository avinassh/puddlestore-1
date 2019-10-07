package raft

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os/user"
	"reflect"
	"runtime"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
)

const (
	WAIT_PERIOD         = 6
	CONCURRENT_REQUESTS = 5
)

func suppressLoggers() {
	if !SHOW_LOGS {
		Out.SetOutput(ioutil.Discard)
		Error.SetOutput(ioutil.Discard)
		Debug.SetOutput(ioutil.Discard)
		grpclog.SetLogger(Out)
	}
}

var SHOW_LOGS bool

func init() {
	flag.BoolVar(&SHOW_LOGS, "raft.showlogs", false, "show student output")
}

// computes argmax over a list of terms
func maxTerm(terms ...uint64) (max uint64) {
	for _, t := range terms {
		if t > max {
			max = t
		}
	}

	return
}

func cleanupStudentCluster(nodes []*RaftNode) {
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		node.server.Stop()
		go func(node *RaftNode) {
			node.GracefulExit()
			node.RemoveLogs()
		}(node)
	}
	time.Sleep(2 * time.Second)
}

// Stops all the grpc servers, removes raftlogs, and closes client connections
func cleanupMockCluster(student *RaftNode, mockRafts []*MockRaft) {
	student.server.Stop()
	go func(student *RaftNode) {
		student.GracefulExit()
		student.RemoveLogs()
	}(student)

	time.Sleep(2 * time.Second)

	for _, n := range mockRafts {
		n.Stop()
		n.RemoteSelf.RemoveClientConn()
	}

	student.GetRemoteSelf().RemoveClientConn()
}

type Vote int

const (
	YES Vote = iota
	NO
	HANG
	ERROR
)

func (v Vote) String() string {
	switch v {
	case YES:
		return "yes"
	case NO:
		return "no"
	case HANG:
		return "hang"
	case ERROR:
		return "error"
	default:
		return "unknown"
	}
}

func (m *MockRaft) validateVoteRequest(req *RequestVoteRequest) {
	if req == nil {
		m.t.Fatal("student sent nil RequestVoteRequest!")
		return
	}

	if currTerm := m.StudentRaft.GetCurrentTerm(); req.Term != currTerm {
		m.t.Fatalf("term in student request was %v, but student term is %v", req.Term, currTerm)
		return
	}

	lastLogIndex := m.StudentRaft.getLastLogIndex()
	lastLogTerm := m.StudentRaft.getLogEntry(lastLogIndex).GetTermId()

	if lastLogIndex != req.LastLogIndex {
		m.t.Fatalf("index in student request was %v, but student last log index is %v", req.LastLogIndex, lastLogIndex)
		return
	}

	if lastLogTerm != req.LastLogTerm {
		m.t.Fatalf("last term in student request was %v, but student last log term is %v", req.LastLogTerm, lastLogTerm)
		return
	}
}

// Set a vote handler based on the given VOTE
func (m *MockRaft) setVoteHandler(v Vote) (sendVote chan bool) {
	sendVote = nil

	switch v {
	case YES:
		m.RequestVote = func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
			m.validateVoteRequest(req)
			return &RequestVoteReply{
				VoteGranted: true,
				Term:        req.GetTerm(),
			}, nil
		}
	case NO:
		m.RequestVote = func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
			m.validateVoteRequest(req)
			return &RequestVoteReply{
				VoteGranted: false,
				Term:        req.GetTerm(),
			}, nil
		}
	case ERROR:
		m.RequestVote = func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
			m.validateVoteRequest(req)
			return nil, MockError
		}
	case HANG:
		sendVote = make(chan bool, 1)
		m.RequestVote = func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
			m.validateVoteRequest(req)

			send, ok := <-sendVote
			if !ok {
				send = false
			}

			return &RequestVoteReply{
				VoteGranted: send,
				Term:        req.GetTerm(),
			}, nil
		}
	}

	return
}

func (m *MockRaft) addRequest(req *AppendEntriesRequest) {
	m.receivedMutex.Lock()
	defer m.receivedMutex.Unlock()
	m.receivedReqs = append(m.receivedReqs, req)
}

func (a *AppendEntriesRequest) isHeartbeat() bool {
	return len(a.GetEntries()) == 0
}

func (m *MockRaft) validateAppendEntries(req *AppendEntriesRequest) {
	// outline
	//     type AppendEntriesRequest struct {
	//     // The leader's term
	//     Term uint64 `protobuf:"varint,1,opt,name=term" json:"term,omitempty"`
	//     // Address of the leader sending this request
	//     Leader *RemoteNode `protobuf:"bytes,2,opt,name=leader" json:"leader,omitempty"`
	//     // The index of the log entry immediately preceding the new ones
	//     PrevLogIndex uint64 `protobuf:"varint,3,opt,name=prevLogIndex" json:"prevLogIndex,omitempty"`
	//     // The term of the log entry at prevLogIndex
	//     PrevLogTerm uint64 `protobuf:"varint,4,opt,name=prevLogTerm" json:"prevLogTerm,omitempty"`
	//     // The log entries the follower needs to store. Empty for heartbeat messages.
	//     Entries []*LogEntry `protobuf:"bytes,5,rep,name=entries" json:"entries,omitempty"`
	//     // The leader's commitIndex
	//     LeaderCommit uint64 `protobuf:"varint,6,opt,name=leaderCommit" json:"leaderCommit,omitempty"`
	// }
	m.addRequest(req)

	if req == nil {
		m.t.Fatal("student sent nil AppendEntriesRequest!")
	}

	if currTerm := m.StudentRaft.GetCurrentTerm(); req.Term != currTerm {
		m.t.Fatalf("term in student request was %v, but student term is %v", req.Term, currTerm)
	}

	if studentRemote := m.StudentRaft.GetRemoteSelf(); studentRemote.Id != req.Leader.Id {
		m.t.Fatalf("leader in student request was %v, but student is %v", req.Leader, studentRemote)
	}

	m.StudentRaft.leaderMutex.Lock()
	if m.StudentRaft.commitIndex != req.LeaderCommit {
		m.t.Fatalf("commit in student request was %v, but student commitIndex is %v", req.Leader, m.StudentRaft.commitIndex)
	}
	m.StudentRaft.leaderMutex.Unlock()
}

func createTestCluster(ports []int) ([]*RaftNode, error) {
	runtime.GOMAXPROCS(8)
	SetDebug(false)
	config := DefaultConfig()
	config.ClusterSize = 5
	config.ElectionTimeout = time.Millisecond * 400
	// Use a path in /tmp/ so we use local disk and not NFS
	curUser, _ := user.Current()
	config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	if runtime.GOOS == "windows" {
		config.LogPath = "raftlogs/"
	}
	return CreateDefinedLocalCluster(config, ports)
}

func startTestCluster(nodes []*RaftNode, t *testing.T) (*RaftNode, uint64) {
	time.Sleep(time.Second * WAIT_PERIOD) // Long enough for timeout+election

	// Test: A follower should time out and becomes leader
	leader, err := findLeader(nodes)
	if err != nil {
		t.Fatalf("Failed to find leader: %v", err)
	}

	// Test: Check for leader's no-op entry on everyone
	leaderIndex := leader.getLastLogIndex()
	leaderTerm := leader.GetCurrentTerm()

	// There should be an entry appended. There might be two if
	// there was a split election during testing.
	if leaderIndex == 0 || leaderIndex > 2 {
		t.Errorf("Leader's last log index is not 1 or 2 (split vote), but %v", leaderIndex)
		// Don't return here, since we can still continue with testing.
	}
	for _, node := range nodes {
		index := node.getLastLogIndex()
		if index != leaderIndex {
			t.Fatalf("One of the nodes most recent index (%v) isn't the same as the leader's (%v)", index, leaderIndex)
		}
		entry := node.getLogEntry(index)
		if entry.Type != CommandType_NOOP {
			t.Fatalf("Last log entry is not a no-op: %v", entry.Command)
		}
	}

	// Test that elections don't take too long to finish
	if leaderTerm > 5 {
		t.Fatalf("Cluster took more than 5 terms to elect a leader: took %v terms", leaderTerm)
	}

	return leader, leaderTerm
}

func findLeader(nodes []*RaftNode) (*RaftNode, error) {
	leaders := make([]*RaftNode, 0)
	for _, node := range nodes {
		if node.State == LEADER_STATE {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) == 0 {
		return nil, fmt.Errorf("No leader found in slice of nodes")
	} else if len(leaders) == 1 {
		return leaders[0], nil
	} else {
		panic(fmt.Sprintf("Found too many leaders in slice of nodes: %v", len(leaders)))
	}
}

func findFollower(nodes []*RaftNode) *RaftNode {
	for _, node := range nodes {
		if node.State == FOLLOWER_STATE {
			return node
		}
	}
	panic("Couldn't find any followers in findFollower!")
}

func logsCorrect(leader *RaftNode, nodes []*RaftNode) bool {
	for _, node := range nodes {
		if node.State != LEADER_STATE {
			return bytes.Compare(node.stateMachine.GetState().([]byte), leader.stateMachine.GetState().([]byte)) == 0
		}
	}
	return true
}

func checkStableStateEquality(ss1 StableState, ss2 StableState) bool {
	if ss1.CurrentTerm != ss2.CurrentTerm {
		return false
	}
	if ss1.VotedFor != ss2.VotedFor {
		return false
	}
	if !reflect.DeepEqual(ss1.ClientReplyCache, ss2.ClientReplyCache) {
		return false
	}
	if ss1.RemoteSelf != ss2.RemoteSelf {
		return false
	}
	if len(ss1.NodeList) != len(ss2.NodeList) {
		return false
	}
	for i := range ss1.NodeList {
		if ss1.NodeList[i] != ss2.NodeList[i] {
			return false
		}
	}
	return true
}

func checkLogCacheEquality(lc1 LogCache, lc2 LogCache) bool {
	if (lc1 == nil && lc2 != nil) || (lc1 != nil && lc2 == nil) {
		return false
	}
	if len(lc1) != len(lc2) {
		return false
	}

	if !reflect.DeepEqual(lc1, lc2) {
		return false
	}
	return true
}
