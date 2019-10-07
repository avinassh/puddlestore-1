package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"golang.org/x/net/context"
)

type heartbeatTestCase struct {
	title               string
	expectedCommitIndex uint64
	numEntries          []int
	maxLogLengths       []int
}

func (tc heartbeatTestCase) run(t *testing.T, setup func(*testing.T, []int) (*RaftNode, []*MockRaft, error)) {
	t.Run(tc.title, func(t *testing.T) {
		student, mocks, err := setup(t, tc.maxLogLengths)
		if err != nil {
			t.Fatal("failed to setup cluster")
		}
		defer cleanupMockCluster(student, mocks)
		timer := time.AfterFunc(20*student.config.HeartbeatTimeout, func() {
			t.Fatal("ran too long (> 20 heartbeats)")
		})
		defer timer.Stop()

		for stage := 0; stage < len(tc.numEntries); stage++ {
			studentLLI := student.getLastLogIndex()
			for i := 0; i < tc.numEntries[stage]; i++ {
				err := student.appendLogEntry(LogEntry{
					TermId: student.GetCurrentTerm(),
					Index:  uint64(int(studentLLI) + i + 1),
					Type:   CommandType_NOOP,
					Data:   []byte{},
				})

				if err != nil {
					t.Fatal("failed to append log entry to student", err)
				}
			}

			// each stage is one heartbeat
			time.Sleep(student.config.HeartbeatTimeout)

			if student.State != LEADER_STATE {
				t.Errorf("(stage %v) student was not in leader state, was: %v", stage+1, student.State)
			}
		}

		time.Sleep(student.config.HeartbeatTimeout * 4)

		student.leaderMutex.Lock()
		if student.commitIndex != tc.expectedCommitIndex {
			t.Errorf("student commit was %v, expecting %v", student.commitIndex, tc.expectedCommitIndex)
		}
		student.leaderMutex.Unlock()
	})
}

func randomizedHeartbeatTestCases(n int, clusterSize int) []heartbeatTestCase {
	cases := make([]heartbeatTestCase, n)

	for i := 0; i < n; i++ {
		var buf bytes.Buffer
		numEntries := rand.Int()%10 + 1
		fmt.Fprintf(&buf, "%v entries/", numEntries)

		maxes := make([]int, clusterSize-1)
		willAppend := make([]int, clusterSize-1)

		fmt.Fprint(&buf, "logs")

		for j := 0; j < len(maxes); j++ {
			maxes[j] = rand.Int()%10 + 1
			if maxes[j] < numEntries+1 {
				willAppend[j] = maxes[j]
			} else {
				willAppend[j] = numEntries + 1
			}

			fmt.Fprint(&buf, " ", maxes[j])
		}

		entries := make([]int, numEntries)
		for j := 0; j < len(entries); j++ {
			entries[j] = 1
		}

		sort.Sort(sort.IntSlice(willAppend))

		cases[i] = heartbeatTestCase{
			maxLogLengths:       maxes,
			expectedCommitIndex: uint64(willAppend[len(willAppend)/2]),
			numEntries:          entries,
			title:               buf.String(),
		}
	}

	return cases
}

func TestLeaderCommitIndex(t *testing.T) {
	suppressLoggers()

	// set a MockRaft to accept entries until its log is over a max length
	setAppendFuncCounter := func(m *MockRaft, maxLogLength int) {
		log := make([]*LogEntry, 0, maxLogLength)
		m.AppendEntries = func(ctx context.Context, req *AppendEntriesRequest) (reply *AppendEntriesReply, err error) {
			m.validateAppendEntries(req)
			reply = &AppendEntriesReply{}
			reply.Term = req.Term

			if len(log) < maxLogLength {
				log = append(log[0:req.PrevLogIndex], req.Entries...)
				reply.Success = true
			} else {
				err = MockError
				reply = nil
			}

			return
		}
	}

	setup := func(t *testing.T, responses []int) (student *RaftNode, mocks []*MockRaft, err error) {
		// Create mock cluster with one student node, and mocks that always vote yes
		config := DefaultConfig()
		config.ClusterSize = len(responses) + 1
		config.ElectionTimeout = 200 * time.Millisecond
		config.HeartbeatTimeout = 200 * time.Millisecond
		student, mocks, err = MockCluster(false, config, t)
		if err != nil {
			t.Error("Couldn't set up mock cluster", err)
		}

		for i, m := range mocks {
			setAppendFuncCounter(m, responses[i])
			err = m.JoinCluster()
			if err != nil {
				t.Fatal("failed to join cluster")
			}
		}

		time.Sleep(200 * time.Millisecond)

		// Ensure student node becomes leader
		retries := 3
		for i := 0; i < retries; i++ {
			time.Sleep(config.ElectionTimeout * 4)
			if student.State != LEADER_STATE {
				t.Logf("(retry %v) student state was not leader, was: %v", i+1, student.State)
			} else {
				break
			}
		}

		if student.State != LEADER_STATE {
			t.Fatal("not able to make student leader")
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
		student.setCurrentTerm(3)

		return
	}

	cases := []heartbeatTestCase{
		{"3 all succeed", 2, []int{1}, []int{5, 5}},
		{"3 all fail", 1, []int{1}, []int{1, 1}},
		{"3 first fails", 2, []int{1}, []int{1, 2}},
		{"3 second fails", 2, []int{1}, []int{2, 1}},

		{"5 one fails", 2, []int{1}, []int{2, 1, 2, 2}},
		{"5 two fails", 2, []int{1}, []int{2, 1, 1, 2}},
		{"5 three fails", 1, []int{1}, []int{2, 1, 1, 1}},
	}

	t.Run("one_stage", func(t *testing.T) {
		for _, tc := range cases {
			tc.run(t, setup)
		}
	})

	cases2 := []heartbeatTestCase{
		{"3 all succeed", 3, []int{1, 1}, []int{5, 5}},
		{"3 all fail", 1, []int{1, 1}, []int{1, 1}},
		{"3 first stage succeeds second fails", 2, []int{1, 1}, []int{2, 2}},
		{"3 only one node succeeds in first stage", 3, []int{1, 1}, []int{1, 5}},
		{"3 only one node succeeds in first stage", 3, []int{1, 1}, []int{5, 1}},
		{"5 computes median", 2, []int{1, 3}, []int{1, 2, 2, 4}},
		{"5 computes median", 3, []int{2, 3}, []int{1, 2, 3, 4}},
		{"5 computes median", 6, []int{2, 3}, []int{1, 2, 5, 5}},
	}

	t.Run("two_stage", func(t *testing.T) {
		for _, tc := range cases2 {
			tc.run(t, setup)
		}
	})

	if t.Failed() {
		// don't run randomized tests if it's already failed
		t.Log("skipping randomized tests")

		return
	}

	cases3 := randomizedHeartbeatTestCases(20, 5)
	t.Run("randomized", func(t *testing.T) {
		t.Log("running", len(cases3), "randomized tests with 5 nodes")
		for _, tc := range cases3 {
			tc.run(t, func(t *testing.T, responses []int) (*RaftNode, []*MockRaft, error) {
				t.Parallel()
				return setup(t, responses)
			})
		}
	})

}
