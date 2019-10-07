package raft

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

// making sure the mock impl works
func TestSetupMock(t *testing.T) {
	t.SkipNow()
	student, mocks, err := MockCluster(true, nil, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}

	t.Log("Created student node:", student)
	t.Log("Created mock nodes:", mocks)

	time.Sleep(DefaultConfig().ElectionTimeout * 2)
	t.Log("Student node is:", student.State)
	time.Sleep(DefaultConfig().ElectionTimeout * 2)
	t.Log("Student node is:", student.State)
}

// more complex example test
func TestSetupReplaceMock(t *testing.T) {
	t.SkipNow()
	student, mocks, err := MockCluster(false, nil, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}

	// Create a new impl for an rpc function
	denyVote := func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
		return &RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
	}

	// replace the existing impl
	mocks[0].RequestVote = denyVote
	mocks[1].RequestVote = denyVote

	mocks[0].JoinCluster()
	mocks[1].JoinCluster()

	time.Sleep(DefaultConfig().ElectionTimeout * 4)

	t.Log("Student node is:", student.State)

	if student.State != CANDIDATE_STATE {
		t.Error("student state was not candidate, was:", student.State)
	}

	// test as part of an rpc function
	mocks[0].RequestVote = func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
		t.Logf("Mock 0 recieved request vote: last_idx: %v term: %v", req.GetLastLogIndex(), req.GetLastLogTerm())
		if req.GetLastLogIndex() != 0 || req.GetLastLogTerm() != 0 {
			t.Errorf("Student node failed to request vote correctly: last_idx: %v term: %v", req.GetLastLogIndex(), req.GetLastLogTerm())
		}

		if term := student.GetCurrentTerm(); req.GetTerm() != term {
			t.Errorf("Student node sent the wrong term: (sent %v, expecting %v)", req.GetTerm(), term)
		}
		return denyVote(ctx, req)
	}

	time.Sleep(DefaultConfig().ElectionTimeout * 5)
}
