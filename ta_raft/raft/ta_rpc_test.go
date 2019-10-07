package raft

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestJoinNodeRPC(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	// Start a mock cluster without joining the nodes
	student, mocks, err := MockCluster(false, config, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}

	//Disable communication from mock 0 to student node
	student.NetworkPolicy.RegisterPolicy(*mocks[0].RemoteSelf, *student.GetRemoteSelf(), false)

	//  join nodes
	for i, m := range mocks {
		err = m.JoinCluster()
		if i == 0 {
			if err == nil {
				t.Fatalf("Should have gotten an error when joining node 0")
			} else {
				errString := grpc.ErrorDesc(err)
				if errString != ErrorNetworkPolicyDenied.Error() {
					t.Fatalf("Should have gotten %v, got %v", ErrorNetworkPolicyDenied, errString)
				}
			}
		} else if err != nil {
			t.Fatal("Failed to setup mock cluster", err)
		}
	}
	time.Sleep(2 * time.Second)
	cleanupMockCluster(student, mocks)
}

func TestStartNodeRPC(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	// Start a mock cluster without joining the nodes
	student, mocks, err := MockCluster(false, config, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}
	defer cleanupMockCluster(student, mocks)

	callCounter := 0
	newStartNodeFunc := func(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
		fmt.Printf("HERE!!\n")
		callCounter = callCounter + 1
		return &Ok{Ok: true}, nil
	}

	mocks[0].StartNode = newStartNodeFunc

	//Disable communication from student node to mock 0
	student.NetworkPolicy.RegisterPolicy(*student.GetRemoteSelf(), *mocks[0].RemoteSelf, false)
	//  join nodes
	for _, m := range mocks {
		err = m.JoinCluster()
		if err != nil {
			t.Fatal("Failed to setup mock cluster", err)
		}
	}
	time.Sleep(2 * time.Second)
	if callCounter > 0 {
		t.Fatalf("StartNode should not have been called, but counter is %v\n", callCounter)
	}
}

func TestAppendEntriesRPC(t *testing.T) {
	nodes, err := createTestCluster([]int{4011, 4012, 4013, 4014, 4015})

	if err != nil {
		t.Fatalf("Could not create cluster: %v", err)
	}
	defer cleanupStudentCluster(nodes)

	leader, _ := startTestCluster(nodes, t)

	follower := findFollower(nodes)

	request := AppendEntriesRequest{
		Term:         uint64(leader.GetCurrentTerm()),
		Leader:       leader.GetRemoteSelf(),
		PrevLogIndex: uint64(leader.getLastLogIndex()),
		PrevLogTerm:  uint64(leader.getLogEntry(leader.getLastLogIndex()).GetTermId()),
		Entries:      nil,
		LeaderCommit: uint64(leader.commitIndex),
	}

	//Disable communication from leader node to follower
	follower.NetworkPolicy.RegisterPolicy(*leader.GetRemoteSelf(), *follower.GetRemoteSelf(), false)

	_, err = follower.AppendEntriesCaller(context.Background(), &request)

	if err == nil {
		t.Fatalf("Should have gotten an error when sending append entries to node from AppendEntriesCaller")
	} else {
		errString := grpc.ErrorDesc(err)
		if errString != ErrorNetworkPolicyDenied.Error() {
			t.Fatalf("Should have gotten %v, got %v", ErrorNetworkPolicyDenied, errString)
		}
	}

	follower.GetRemoteSelf().AppendEntriesRPC(leader, &request)
	if err == nil {
		t.Fatalf("Should have gotten an error when sending append entries to node from AppendEntriesRPC")
	} else {
		errString := grpc.ErrorDesc(err)
		if errString != ErrorNetworkPolicyDenied.Error() {
			t.Fatalf("Should have gotten %v, got %v", ErrorNetworkPolicyDenied, errString)
		}
	}
}

func TestRequestVotesRPC(t *testing.T) {
	nodes, err := createTestCluster([]int{4011, 4012, 4013, 4014, 4015})

	if err != nil {
		t.Fatalf("Could not create cluster: %v", err)
	}
	defer cleanupStudentCluster(nodes)

	leader, _ := startTestCluster(nodes, t)

	follower := findFollower(nodes)

	request := RequestVoteRequest{
		Candidate:    leader.GetRemoteSelf(),
		Term:         leader.GetCurrentTerm(),
		LastLogIndex: leader.getLastLogIndex() + 1,
		LastLogTerm:  leader.getLogEntry(leader.getLastLogIndex()).GetTermId() + 1,
	}

	//Disable communication from leader node to follower
	follower.NetworkPolicy.RegisterPolicy(*leader.GetRemoteSelf(), *follower.GetRemoteSelf(), false)

	_, err = follower.RequestVoteCaller(context.Background(), &request)

	if err == nil {
		t.Fatalf("Should have gotten an error when sending request vote to node from RequestVoteCaller")
	} else {
		errString := grpc.ErrorDesc(err)
		if errString != ErrorNetworkPolicyDenied.Error() {
			t.Fatalf("Should have gotten %v, got %v", ErrorNetworkPolicyDenied, errString)
		}
	}

	follower.GetRemoteSelf().RequestVoteRPC(leader, &request)
	if err == nil {
		t.Fatalf("Should have gotten an error when sending request vote to node from RequestVoteRPC")
	} else {
		errString := grpc.ErrorDesc(err)
		if errString != ErrorNetworkPolicyDenied.Error() {
			t.Fatalf("Should have gotten %v, got %v", ErrorNetworkPolicyDenied, errString)
		}
	}
}

var interceptorError = "Interceptor Error"

var RPCTestInterceptor grpc.UnaryClientInterceptor = func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return fmt.Errorf(interceptorError)
}

func TestRPCConnCheckErrorHandling(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 3
	// Start a mock cluster without joining the nodes
	student, mocks, err := MockCluster(true, config, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}
	defer cleanupMockCluster(student, mocks)

	// Add interceptor between student and mock[0]
	dialOptionsLocal := append(dialOptions, grpc.WithUnaryInterceptor(RPCTestInterceptor))
	newCC, err := grpc.Dial(mocks[0].RemoteSelf.Addr, dialOptionsLocal...)
	if err != nil {
		t.Errorf("grpc dial error: %v\n", err)
	}
	connMapLock.Lock()
	connMap[mocks[0].RemoteSelf.Addr] = newCC
	connMapLock.Unlock()

	_, err = mocks[0].RemoteSelf.AppendEntriesRPC(student, nil)
	if err != nil {
		if err.Error() != interceptorError {
			t.Fatalf("(AppendEntries) Returned error should have been: %v, but was %v\n", interceptorError, err)
		}
		// check if connection was closed by calling close on it again and making sure it errors
		if cc, ok := connMap[mocks[0].RemoteSelf.Addr]; ok {
			err = cc.Close()
			if err == nil {
				t.Fatalf("(AppendEntries) connection wasn't originally closed when RPC failed\n")
			}
		}
	}

	connMapLock.Lock()
	connMap[mocks[0].RemoteSelf.Addr] = newCC
	connMapLock.Unlock()

	_, err = mocks[0].RemoteSelf.RequestVoteRPC(student, nil)
	if err != nil {
		if err.Error() != interceptorError {
			t.Fatalf("(RequestVote) Returned error should have been: %v, but was %v\n", interceptorError, err)
		}
		// check if connection was closed by calling close on it again and making sure it errors
		if cc, ok := connMap[mocks[0].RemoteSelf.Addr]; ok {
			err = cc.Close()
			if err == nil {
				t.Fatalf("(RequestVote) connection wasn't originally closed when RPC failed\n")
			}
		}
	}
}
