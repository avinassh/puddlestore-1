package raft

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestReceiveHigherTermRequests(t *testing.T) {
	suppressLoggers()

	t.Run("follower", func(t *testing.T) {
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

		t.Run("in RequestVote", func(t *testing.T) {
			// In the case where node should vote for the new candidate
			currentTerm := student.GetCurrentTerm()
			lli := student.getLastLogIndex()
			llt := student.getLogEntry(lli).GetTermId()
			reply, _ := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: lli,
				LastLogTerm:  llt,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have increased term")
			}

			// Case where node shouldn't vote for the new candidate
			currentTerm = student.GetCurrentTerm()
			reply, err = mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have increased term")
			}

			// Other case where node shouldn't vote for the new candidate (same index diff term)
			currentTerm = student.GetCurrentTerm()
			lli = student.getLastLogIndex()
			llt = student.getLogEntry(lli).GetTermId()
			reply, err = mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: lli,
				LastLogTerm:  llt - 1,
			})

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have increased term")
			}
		})

		t.Run("in AppendEntries", func(t *testing.T) {
			// Case where appendentries could be coming from new leader
			currentTerm := student.GetCurrentTerm()
			lli := student.getLastLogIndex()
			llt := student.getLogEntry(lli).GetTermId()
			reply, _ := mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
				Leader:       mocks[0].RemoteSelf,
				Entries:      []*LogEntry{},
				PrevLogIndex: lli,
				PrevLogTerm:  llt,
				LeaderCommit: 0,
				Term:         currentTerm + 1,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have increased term")
			}

			// Other case where student log is "out of date"
			// We do this to test to make sure that the student code doesn't short circuit
			// their append entries handler if the local term doesn't match with the sender's term
			currentTerm = student.GetCurrentTerm()
			lli = student.getLastLogIndex()
			llt = student.getLogEntry(lli).GetTermId()
			reply, _ = mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
				Leader:       mocks[0].RemoteSelf,
				Entries:      []*LogEntry{},
				PrevLogIndex: lli + 1,
				PrevLogTerm:  llt,
				LeaderCommit: 0,
				Term:         currentTerm + 1,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have increased term")
			}
		})
	})

	t.Run("leader", func(t *testing.T) {
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

		t.Run("in RequestVote", func(t *testing.T) {
			// In the case where node should vote for the new candidate
			currentTerm := student.GetCurrentTerm()
			lli := student.getLastLogIndex()
			llt := student.getLogEntry(lli).GetTermId()
			reply, _ := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: lli,
				LastLogTerm:  llt,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have stepped down")
			}

			// Let it become leader again
			time.Sleep(2 * time.Second)
			if student.State != LEADER_STATE {
				t.Fatal("Student should have become leader again")
			}

			// Case where node shouldn't vote for the new candidate
			currentTerm = student.GetCurrentTerm()
			reply, err = mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have stepped down")
			}

			// Let it become leader again
			time.Sleep(2 * time.Second)
			if student.State != LEADER_STATE {
				t.Fatal("Student should have become leader again")
			}

			// Other case where node shouldn't vote for the new candidate (same index diff term)
			currentTerm = student.GetCurrentTerm()
			lli = student.getLastLogIndex()
			llt = student.getLogEntry(lli).GetTermId()
			reply, err = mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: lli,
				LastLogTerm:  llt - 1,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have stepped down")
			}
		})

		t.Run("in AppendEntries", func(t *testing.T) {
			// Case where appendentries could be coming from new leader
			currentTerm := student.GetCurrentTerm()
			lli := student.getLastLogIndex()
			llt := student.getLogEntry(lli).GetTermId()
			reply, _ := mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
				Leader:       mocks[0].RemoteSelf,
				Entries:      []*LogEntry{},
				PrevLogIndex: lli,
				PrevLogTerm:  llt,
				LeaderCommit: 0,
				Term:         currentTerm + 1,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have stepped down")
			}

			// Let it become leader again
			time.Sleep(2 * time.Second)
			if student.State != LEADER_STATE {
				t.Fatal("Student should have become leader again")
			}

			// Other case where student log is "out of date"
			// We do this to test to make sure that the student code doesn't short circuit
			// their append entries handler if the local term doesn't match with the sender's term
			currentTerm = student.GetCurrentTerm()
			lli = student.getLastLogIndex()
			llt = student.getLogEntry(lli).GetTermId()
			reply, _ = mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
				Leader:       mocks[0].RemoteSelf,
				Entries:      []*LogEntry{},
				PrevLogIndex: lli + 1,
				PrevLogTerm:  llt,
				LeaderCommit: 0,
				Term:         currentTerm + 1,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have stepped down")
			}
		})

	})

	t.Run("candidate", func(t *testing.T) {
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

		t.Run("in RequestVote", func(t *testing.T) {
			// In the case where node should vote for the new candidate
			currentTerm := student.GetCurrentTerm()
			lli := student.getLastLogIndex()
			llt := student.getLogEntry(lli).GetTermId()
			reply, _ := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 10000, // b/c candidate term is always growing
				LastLogIndex: lli,
				LastLogTerm:  llt,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+10000 {
				t.Fatal("Student should have stepped down")
			}

			// Let it become candidate again
			time.Sleep(2 * time.Second)
			if student.State != CANDIDATE_STATE {
				t.Fatal("Student should have become candidate again")
			}

			// Case where node shouldn't vote for the new candidate
			currentTerm = student.GetCurrentTerm()
			reply, err = mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 10000,
				LastLogIndex: 0,
				LastLogTerm:  0,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+10000 {
				t.Fatal("Student should have stepped down")
			}

			// Let it become candidate again
			time.Sleep(2 * time.Second)
			if student.State != CANDIDATE_STATE {
				t.Fatal("Student should have become candidate again")
			}

			// Other case where node shouldn't vote for the new candidate (same index diff term)
			currentTerm = student.GetCurrentTerm()
			lli = student.getLastLogIndex()
			llt = student.getLogEntry(lli).GetTermId()
			reply, err = mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         currentTerm + 1,
				LastLogIndex: lli,
				LastLogTerm:  llt - 1,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+1 {
				t.Fatal("Student should have stepped down")
			}
		})

		t.Run("in AppendEntries", func(t *testing.T) {
			// Case where appendentries could be coming from new leader
			currentTerm := student.GetCurrentTerm()
			lli := student.getLastLogIndex()
			llt := student.getLogEntry(lli).GetTermId()
			reply, _ := mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
				Leader:       mocks[0].RemoteSelf,
				Entries:      []*LogEntry{},
				PrevLogIndex: lli,
				PrevLogTerm:  llt,
				LeaderCommit: 0,
				Term:         currentTerm + 10000,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+10000 {
				t.Fatal("Student should have stepped down")
			}

			// Let it become candidate again
			time.Sleep(2 * time.Second)
			if student.State != CANDIDATE_STATE {
				t.Fatal("Student should have become candidate again")
			}

			// Other case where student log is "out of date"
			currentTerm = student.GetCurrentTerm()
			lli = student.getLastLogIndex()
			llt = student.getLogEntry(lli).GetTermId()
			reply, _ = mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &AppendEntriesRequest{
				Leader:       mocks[0].RemoteSelf,
				Entries:      []*LogEntry{},
				PrevLogIndex: lli + 1,
				PrevLogTerm:  llt,
				LeaderCommit: 0,
				Term:         currentTerm + 10000,
			})

			time.Sleep(time.Millisecond * 50)

			if student.State != FOLLOWER_STATE || reply.GetTerm() != currentTerm+10000 {
				t.Fatal("Student should have stepped down")
			}
		})
	})
}

func TestReceiveHigherTermResponses(t *testing.T) {
	t.Run("leader", func(t *testing.T) {
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

		t.Run("from AppendEntries", func(t *testing.T) {
			currentTerm := student.GetCurrentTerm()

			// Create new impls for rpc functions
			higherTermResponse := func(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
				reply := &AppendEntriesReply{}
				reply.Term = req.Term + 1000
				reply.Success = false
				return reply, nil
			}
			denyVote := func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
				return &RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
			}

			// Configure the mock cluster to not allow the leader to get re-elected if it steps down
			for _, m := range mocks {
				m.RequestVote = denyVote
			}
			// Then configure one of the mock nodes to reply with a higher term to the leader
			mocks[0].AppendEntries = higherTermResponse

			// Wait for the leader to send appendentries and get responses back
			time.Sleep(1 * time.Second)

			if student.State != CANDIDATE_STATE || student.GetCurrentTerm() < currentTerm+1000 {
				t.Errorf("%d %d", currentTerm+1000, student.GetCurrentTerm())
				t.Fatal("Student should have stepped down to a follow and then become a candidate and accepted higher term")
			}
		})
	})

	t.Run("candidate", func(t *testing.T) {
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

		t.Run("from RequestVote", func(t *testing.T) {
			currentTerm := student.GetCurrentTerm()

			// Configure one node to reply with a higher term
			// Uses a buffer to notify the calling function when the candidate
			// should have stepped down
			whenVoted := make(chan bool, 1) // buffered so sending doesn't block
			denyVoteHigherTerm := func(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
				defer func() {
					whenVoted <- true
				}()
				return &RequestVoteReply{Term: req.Term + 1000, VoteGranted: false}, nil
			}
			mocks[0].RequestVote = denyVoteHigherTerm

			// Will block until special node votes
			<-whenVoted

			time.Sleep(5 * time.Millisecond) // give the student some time to process
			// the response

			if student.State != FOLLOWER_STATE || student.GetCurrentTerm() < currentTerm+1000 {
				// fmt.Errorf("%d", student.State)
				t.Errorf("%d %d", student.GetCurrentTerm(), currentTerm+1000)
				t.Fatal("Student should have stepped down")
			}
		})
	})
}

// TODO test that followers increment term when they receive requestVote for higher term,
// and fix our implementation to do this
