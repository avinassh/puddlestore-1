package raft

import (
	"fmt"
	"os"
	"os/user"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/cs138"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/raft/hashmachine"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var MockError error = fmt.Errorf("Error by Mock Raft")

// Default implementation. Does nothing except return true.
func DefaultJoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	return &Ok{Ok: true}, nil
}

// Default implementation. Does nothing except return true.
func DefaultStartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	return &Ok{Ok: true}, nil
}

// Default implementation. Does nothing except return true and echo the term.
func DefaultAppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	reply.Term = req.Term
	reply.Success = true
	return reply, nil
}

// Default implementation. Echoes the term and always votes positively.
func DefaultRequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	reply.Term = req.Term
	reply.VoteGranted = true

	return reply, nil
}

// Default implementation. Returns empty, since it's not useful for testing student code.
func DefaultRegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	return &RegisterClientReply{}, nil
}

// Default implementation. Returns empty, since it's not useful for testing student code.
func DefaultClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return &ClientReply{}, nil
}

// Error implementation. Always returns a MockError.
func ErrorJoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorStartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorAppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorRequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorRegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	return nil, MockError
}

// Error implementation. Always returns a MockError.
func ErrorClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return nil, MockError
}

type MockRaft struct {
	// implements RaftRPCServer
	Join           func(ctx context.Context, r *RemoteNode) (*Ok, error)
	StartNode      func(ctx context.Context, req *StartNodeRequest) (*Ok, error)
	AppendEntries  func(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error)
	RequestVote    func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error)
	RegisterClient func(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error)
	ClientRequest  func(ctx context.Context, req *ClientRequest) (*ClientReply, error)

	RemoteSelf        *RemoteNode
	server            *grpc.Server
	StudentRaft       *RaftNode
	studentRaftClient RaftRPCClient

	receivedReqs  []*AppendEntriesRequest
	receivedMutex sync.Mutex

	t *testing.T
}

func (m *MockRaft) JoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	return m.Join(ctx, r)
}

func (m *MockRaft) StartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	return m.StartNode(ctx, req)
}

func (m *MockRaft) AppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	return m.AppendEntries(ctx, req)
}

func (m *MockRaft) RequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	return m.RequestVote(ctx, req)
}

func (m *MockRaft) RegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	return m.RegisterClient(ctx, req)
}

func (m *MockRaft) ClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return m.ClientRequest(ctx, req)
}

// Stop the grpc server at this MockRaft
func (m *MockRaft) Stop() {
	m.server.Stop()
}

// Call JoinRPC on the student node
func (m *MockRaft) JoinCluster() (err error) {
	_, err = m.studentRaftClient.JoinCaller(context.Background(), m.RemoteSelf)
	return
}

// Create a new mockraft with the default config
func NewDefaultMockRaft(studentRaft *RaftNode) (m *MockRaft, err error) {
	m = &MockRaft{
		Join:           DefaultJoinCaller,
		StartNode:      DefaultStartNodeCaller,
		AppendEntries:  DefaultAppendEntriesCaller,
		RequestVote:    DefaultRequestVoteCaller,
		RegisterClient: DefaultRegisterClientCaller,
		ClientRequest:  DefaultClientRequestCaller,

		StudentRaft: studentRaft,
	}

	err = m.init(DefaultConfig())

	return
}

func (m *MockRaft) init(config *Config) error {
	m.server = grpc.NewServer()
	RegisterRaftRPCServer(m.server, m)
	conn, port, err := cs138.OpenListener()
	if err != nil {
		return err
	}
	hostname, _ := os.Hostname()
	addr := fmt.Sprintf("%v:%v", hostname, port)
	id := AddrToId(addr, config.NodeIdSize)
	m.RemoteSelf = &RemoteNode{Addr: addr, Id: id}

	cc, err := m.StudentRaft.GetRemoteSelf().ClientConn()
	if err != nil {
		return err
	}

	m.studentRaftClient = cc

	go m.server.Serve(conn)
	return nil
}

// set the testing.T associated with this MockRaft
func (m *MockRaft) setTestingT(t *testing.T) {
	m.t = t
}

// append a given number of noops in a given term to the student
func (m *MockRaft) appendNOOPsToStudent(number int, term uint64) bool {
	lastIndex := m.StudentRaft.getLastLogIndex()
	entries := make([]*LogEntry, number)
	for i, _ := range entries {
		entries[i] = &LogEntry{
			TermId: term,
			Index:  lastIndex + uint64(i) + 1,
			Type:   CommandType_NOOP,
			Data:   []byte{},
		}
	}
	reply, err := m.studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
		Leader:       m.RemoteSelf,
		Entries:      entries,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  m.StudentRaft.getLogEntry(lastIndex).GetTermId(),
		LeaderCommit: 0,
		Term:         term,
	})

	success := true

	if err != nil || reply.GetSuccess() == false {
		m.t.Logf("Failed heartbeat to student: %v, reply: %v, currentTerm:%v", err, reply, term)
		success = false
	}

	if m.StudentRaft.State != FOLLOWER_STATE {
		m.t.Fatal("Student was not in follower state after receiving append entries")
	}
	return success
}

// Create a cluster from one student raft and ClusterSize-1 mock rafts, connect the mock rafts to the student one.
func MockCluster(joinThem bool, config *Config, t *testing.T) (studentRaft *RaftNode, mockRafts []*MockRaft, err error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Use a path in /tmp so we use local disk and not NFS
	if runtime.GOOS != "windows" {
		curUser, _ := user.Current()
		config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	}

	studentRaft, err = CreateNode(0, nil, new(hashmachine.HashMachine), config)
	if err != nil {
		return
	}
	mockRafts = make([]*MockRaft, config.ClusterSize-1)

	for i, _ := range mockRafts {
		var mr *MockRaft
		mr, err = NewDefaultMockRaft(studentRaft)
		if err != nil {
			return
		}
		mockRafts[i] = mr
		mr.setTestingT(t)
		if joinThem {
			err = mr.JoinCluster()
			if err != nil {
				return
			}
		}
	}

	if joinThem {
		time.Sleep(110 * time.Millisecond)
	}
	return
}

func createFollowerMockCluster(t *testing.T) (student *RaftNode, mocks []*MockRaft, err error) {
	// Setup config
	config := DefaultConfig()
	config.ElectionTimeout = 60 * time.Second // Set large election timeout
	config.ClusterSize = 3

	// Create cluster
	student, mocks, err = MockCluster(true, config, t)
	if err != nil {
		t.Fatal("couldn't set up mock cluster", err)
	}
	time.Sleep(150 * time.Millisecond)

	// Ensure student node becomes follower
	if student.State != FOLLOWER_STATE {
		t.Fatal("student state was not follower, was: ", student.State)
	}

	// Set follower term
	student.setCurrentTerm(1)

	return
}

func createFollowerMockClusterWith3Entries(t *testing.T) (student *RaftNode, mocks []*MockRaft, err error) {
	student, mocks, err = createFollowerMockCluster(t)
	if err != nil {
		return nil, nil, err
	}

	// Add 3 entries to follower
	studentTerm := student.GetCurrentTerm()
	studentLLI := student.getLastLogIndex()
	for i := 0; i < 3; i++ {
		err = student.appendLogEntry(LogEntry{
			TermId: studentTerm,
			Index:  uint64(int(studentLLI) + i + 1),
			Type:   CommandType_NOOP,
			Data:   []byte{},
		})

		if err != nil {
			t.Fatal("failed to append log entry to student", err)
		}
	}

	return student, mocks, err
}

func createCandidateMockCluster(t *testing.T) (student *RaftNode, mocks []*MockRaft, err error) {
	// Create mock cluster with one student node
	student, mocks, err = MockCluster(false, nil, t)
	if err != nil {
		t.Error("couldn't set up mock cluster", err)
	}

	// Setup mocks to always deny votes
	denyVote := func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
		return &RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
	}
	mocks[0].RequestVote = denyVote
	mocks[1].RequestVote = denyVote
	mocks[0].JoinCluster()
	mocks[1].JoinCluster()

	time.Sleep(100 * time.Millisecond)

	// Ensure student node becomes candidate
	time.Sleep(DefaultConfig().ElectionTimeout * 4)
	if student.State != CANDIDATE_STATE {
		t.Error("student state was not candidate, was:", student.State)
	}

	// Change student node election timeout to ensure term remains the same
	student.config.ElectionTimeout = time.Duration(60 * time.Second)

	// Add entries to student log cache
	numEntries := 3
	studentLLI := student.getLastLogIndex()
	for i := 0; i < numEntries; i++ {
		err = student.appendLogEntry(LogEntry{
			TermId: student.GetCurrentTerm(),
			Index:  uint64(int(studentLLI) + i + 1),
			Type:   CommandType_NOOP,
			Data:   []byte{},
		})

		if err != nil {
			t.Fatal("failed to append log entry to student", err)
		}
	}

	return
}

func createLeaderMockCluster(t *testing.T) (student *RaftNode, mocks []*MockRaft, err error) {
	// Create mock cluster with one student node, and mocks that always vote yes
	student, mocks, err = MockCluster(true, nil, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Ensure student node becomes leader
	time.Sleep(DefaultConfig().ElectionTimeout * 4)
	if student.State != LEADER_STATE {
		t.Error("student state was not leader, was:", student.State)
	}

	// Check student node appends NOOP
	if student.getLastLogIndex() != 1 {
		t.Fatal("student didn't setup log when becoming leader")
	}

	if entry := student.getLogEntry(student.getLastLogIndex()); entry.GetIndex() != 0 &&
		entry.GetTermId() != 1 &&
		entry.GetType() != CommandType_NOOP {
		t.Fatal("student didn't append noop to log")
	}

	// Change student node election timeout to ensure term remains the same
	student.config.ElectionTimeout = time.Duration(60 * time.Second)

	// Add entries to student log cache
	numEntries := 3
	studentLLI := student.getLastLogIndex()
	for i := 0; i < numEntries; i++ {
		err = student.appendLogEntry(LogEntry{
			TermId: student.GetCurrentTerm(),
			Index:  uint64(int(studentLLI) + i + 1),
			Type:   CommandType_NOOP,
			Data:   []byte{},
		})

		if err != nil {
			t.Fatal("failed to append log entry to student", err)
		}
	}

	return
}
