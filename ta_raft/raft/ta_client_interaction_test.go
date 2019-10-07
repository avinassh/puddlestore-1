package raft

import (
	"bytes"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/raft/hashmachine"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestClientInteraction_Leader(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	student, mocks, err := MockCluster(false, config, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}
	defer cleanupMockCluster(student, mocks)

	// set the student raft to be a leader forever
	for _, m := range mocks {
		err = m.JoinCluster()
		if err != nil {
			t.Fatal("Failed to setup mock cluster", err)
		}
	}

	time.Sleep(2 * time.Second)
	if student.State != LEADER_STATE {
		t.Fatal("Student was not in leader state in mock cluster that always votes yes")
	}

	// First make sure we can register a client correctly
	reply, _ := mocks[0].studentRaftClient.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Fatal("Couldn't register client")
	}

	clientid := reply.ClientId

	if student.getLastLogIndex() < clientid || student.getLogEntry(clientid).Type != CommandType_CLIENT_REGISTRATION {
		t.Fatal("Client id doesn't correspond to client registration index")
	}

	if student.commitIndex < clientid || student.lastApplied < clientid {
		t.Fatal("Leader registered client without committing an entry")
	}

	// Now see how sending a client request works
	firstReq := ClientRequest{
		clientid,
		1,
		hashmachine.HASH_CHAIN_INIT,
		[]byte("hello"),
	}
	clientResult, _ := mocks[0].studentRaftClient.ClientRequestCaller(context.Background(), &firstReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Send another request
	clientReq := ClientRequest{
		clientid,
		2,
		hashmachine.HASH_CHAIN_ADD,
		[]byte{},
	}
	clientResult, _ = mocks[0].studentRaftClient.ClientRequestCaller(context.Background(), &clientReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Now verify that those requests are actually in the log cache and committed
	var initIndex = -1
	var addIndex = -1
	var i uint64
	for i = 0; i <= student.getLastLogIndex(); i++ {
		entry := student.getLogEntry(i)
		if entry.Type == CommandType_STATE_MACHINE_COMMAND {
			if entry.Command == hashmachine.HASH_CHAIN_INIT {
				initIndex = int(i)
				if !bytes.Equal(entry.Data, firstReq.Data) {
					t.Fatal("Wrong data in leader log for client request")
				}
				if entry.CacheId != createCacheId(clientid, 1) {
					t.Fatal("Wrong cache id in leader log for client request")
				}
			} else if entry.Command == hashmachine.HASH_CHAIN_ADD {
				addIndex = int(i)
				if entry.CacheId != createCacheId(clientid, 2) {
					t.Fatal("Wrong cache id in leader log for client request")
				}
			}
		}
	}

	if initIndex == -1 {
		t.Fatal("Didn't find client request entry (hash init) in log")
	}
	if addIndex == -1 {
		t.Fatal("Didn't find client request entry (hash add) in log")
	}
	if addIndex <= initIndex {
		t.Fatal("Hash add and init indices out of order")
	}

	if student.commitIndex < uint64(addIndex) || student.lastApplied < uint64(addIndex) {
		t.Fatal("Leader replied to client without committing log entry")
	}

	// Now retry the same request a ton of times and verify that nothing changes
	oldLastLogIndex := student.getLastLogIndex()
	for i := 0; i < 10; i++ {
		clientResult, _ = mocks[0].studentRaftClient.ClientRequestCaller(context.Background(), &clientReq)
		if clientResult.Status != ClientStatus_OK {
			t.Fatal("Retried client request failed")
		}
		if student.getLastLogIndex() > oldLastLogIndex {
			t.Fatal("Retried client request caused a new log to be added")
		}
	}
}

func TestClientInteraction_Candidate(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	student, mocks, err := MockCluster(false, config, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}
	defer cleanupMockCluster(student, mocks)

	// Create a new impl for an rpc function
	denyVote := func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
		return &RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
	}

	// set the student raft to be a candidate forever (always loses)
	for _, m := range mocks {
		m.RequestVote = denyVote
		err = m.JoinCluster()
		if err != nil {
			t.Fatal("Failed to setup mock cluster", err)
		}
	}

	// Let it become a candidate
	time.Sleep(2 * time.Second)
	if student.State != CANDIDATE_STATE {
		t.Fatal("Student was not in candidate state in mock cluster w/ no leader that always votes no")
	}

	// Make sure we can't register a client
	reply, _ := mocks[0].studentRaftClient.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when registering a client to a candidate")
	}

	// Now see how sending a client request works
	firstReq := ClientRequest{
		1,
		1,
		hashmachine.HASH_CHAIN_INIT,
		[]byte("hello"),
	}
	clientResult, _ := mocks[0].studentRaftClient.ClientRequestCaller(context.Background(), &firstReq)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a candidate")
	}
}

func TestClientInteraction_Follower(t *testing.T) {
	config := DefaultConfig()
	config.ElectionTimeout = 60 * time.Second
	config.ClusterSize = 3
	config.HeartbeatTimeout = 500 * time.Millisecond
	student, mocks, err := MockCluster(false, config, t)
	if err != nil {
		t.Fatal("Couldn't set up mock cluster", err)
	}
	defer cleanupMockCluster(student, mocks)

	for _, n := range mocks {
		n.JoinCluster()
	}

	time.Sleep(150 * time.Millisecond)

	// Make sure we can't register a client
	reply, _ := mocks[0].studentRaftClient.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when registering a client to a candidate")
	}

	// Now see how sending a client request works
	firstReq := ClientRequest{
		1,
		1,
		hashmachine.HASH_CHAIN_INIT,
		[]byte("hello"),
	}
	clientResult, _ := mocks[0].studentRaftClient.ClientRequestCaller(context.Background(), &firstReq)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a candidate")
	}
}
