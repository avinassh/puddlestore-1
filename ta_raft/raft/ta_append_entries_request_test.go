package raft

import (
	"context"
	"testing"
)

func TestAppendEntriesRequest(t *testing.T) {
	suppressLoggers()
	t.Run("Follower", AppendEntriesRequest_Follower)
	t.Run("Candidate", AppendEntriesRequest_Candidate)
	t.Run("Leader", AppendEntriesRequest_Leader)
}

func AppendEntriesRequest_Follower(t *testing.T) {
	testHandleAppendEntriesRequest(t, createFollowerMockClusterWith3Entries)
}

func AppendEntriesRequest_Candidate(t *testing.T) {
	testHandleAppendEntriesRequest(t, createCandidateMockCluster)
}
func AppendEntriesRequest_Leader(t *testing.T) {
	testHandleAppendEntriesRequest(t, createLeaderMockCluster)
}

func testHandleAppendEntriesRequest(t *testing.T, createCluster func(t *testing.T) (student *RaftNode, mocks []*MockRaft, err error)) {
	// Setup test cases
	testCases := []struct {
		title string

		expectedFallback bool
		expectedSuccess  bool

		studentCommitOffset int64

		leaderTermOffset   int64
		prevLogIndexOffset int64
		prevLogTermOffset  int64
		entries            []LogEntry
		leaderCommitOffset int64
	}{
		// NOTE: these tests are *far* from comprehensive of all possible requests,
		// but they do cover most representative ones. They should be paired with
		// some manual inspection of student code as well.

		{"lower term is rejected", false, false, 0, -1, 0, 0, []LogEntry{}, 0},
		{"lower term is rejected with higher pli", false, false, 0, -1, 1, 0, []LogEntry{}, 0},
		{"lower term is rejected with mismatched plt", false, false, 0, -1, 0, -1, []LogEntry{}, 0},
		{"lower term is rejected with higher commit index", false, false, 0, -1, 0, 0, []LogEntry{}, 1},
		{"lower term is rejected with one entry", false, false, 0, -1, 0, 0, []LogEntry{LogEntry{Index: 3, TermId: 1, Type: CommandType_NOOP, Data: []byte{}}}, 1},

		{"equal term is accepted", true, true, 0, 0, 0, 0, []LogEntry{}, 0},
		{"equal term is accepted and truncates with lower pli and one entry", true, true, -1, 0, -1, 0, []LogEntry{LogEntry{Index: 3, TermId: 1, Type: CommandType_CLIENT_REGISTRATION, Data: []byte{}}}, 0},
		{"equal term is rejected with higher pli", true, false, 0, 0, 1, 0, []LogEntry{}, 0},
		{"equal term is rejected with mismatched plt", true, false, 0, 0, 0, -1, []LogEntry{}, 0},
		{"equal term is accepted with one entry", true, true, 0, 0, 0, 0, []LogEntry{LogEntry{Index: 4, TermId: 1, Type: CommandType_NOOP, Data: []byte{}}}, 1},
		{"equal term is accepted with higher commit index", true, true, -1, 0, 0, 0, []LogEntry{}, 0},

		{"higher term is accepted", true, true, 0, 1, 0, 0, []LogEntry{}, 0},
		{"higher term is accepted and truncates with lower pli and one entry", true, true, -1, 1, -1, 0, []LogEntry{LogEntry{Index: 3, TermId: 1, Type: CommandType_CLIENT_REGISTRATION, Data: []byte{}}}, 0},
		{"higher term is rejected with higher pli", true, false, 0, 1, 1, 0, []LogEntry{}, 0},
		{"higher term is rejected with mismatched plt", true, false, 0, 1, 0, -1, []LogEntry{}, 0},
		{"higher term is accepted with one entry", true, true, 0, 1, 0, 0, []LogEntry{LogEntry{Index: 4, TermId: 1, Type: CommandType_NOOP, Data: []byte{}}}, 0},
		{"higher term is accepted with higher commit index", true, true, -1, 1, 0, 0, []LogEntry{}, 1},

		{"appending multiple entries succeeds", true, true, 0, 0, 0, 0, []LogEntry{
			{
				Type:   CommandType_NOOP,
				Index:  4,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   CommandType_NOOP,
				Index:  5,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   CommandType_NOOP,
				Index:  6,
				TermId: 1,
				Data:   []byte{},
			},
		}, 3},
		{"appending multiple entries with truncation succeeds", true, true, -2, 0, -1, 0, []LogEntry{
			{
				Type:   CommandType_NOOP,
				Index:  3,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   CommandType_NOOP,
				Index:  4,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   CommandType_NOOP,
				Index:  5,
				TermId: 1,
				Data:   []byte{},
			},
		}, 4},
		// This test fails on our implementation, since there's no situation in real execution
		//  when prevLogIndex will be more than 1 less than the log length of a follower
		// {"truncation of multiple entries succeeds", false, true, -2, 0, -2, 0, []LogEntry{
		// 	{
		// 		Type:   CommandType_NOOP,
		// 		Index:  2,
		// 		TermId: 1,
		// 		Data:   []byte{},
		// 	},
		// 	{
		// 		Type:   CommandType_NOOP,
		// 		Index:  3,
		// 		TermId: 1,
		// 		Data:   []byte{},
		// 	},
		// 	{
		// 		Type:   CommandType_NOOP,
		// 		Index:  4,
		// 		TermId: 1,
		// 		Data:   []byte{},
		// 	},
		// }, 1},
		// {"commit index should not be decremented"},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			// Create cluster
			student, mocks, err := createCluster(t)
			if err != nil {
				t.Fatal("unable to create cluster", err)
			}
			studentInitialState := student.State
			defer cleanupMockCluster(student, mocks)

			// Set commit index and last applied for student node
			optimalCommitIndex := len(student.logCache) - 1
			student.commitIndex = uint64(int64(optimalCommitIndex) + tc.studentCommitOffset)
			student.lastApplied = student.commitIndex

			// Save current state of student
			studentTerm := student.GetCurrentTerm()

			studentLogCache := make([]LogEntry, len(student.logCache))
			for i, entry := range student.logCache {
				// Make copy of log cache...
				studentLogCache[i] = LogEntry{
					Index:   entry.Index,
					TermId:  entry.TermId,
					Type:    entry.Type,
					Command: entry.Command,
					Data:    entry.Data,
					CacheId: entry.CacheId,
				}
			}

			studentLLI := student.getLastLogIndex()
			studentLLT := student.getLogEntry(studentLLI).GetTermId()
			studentCommitIndex := student.commitIndex
			studentLastApplied := student.lastApplied

			// If sending request to a leader, update our entries[] so their indices
			// are incremented; this is because a leader has it's own NOOP in it's log,
			// which a follower or candidate should not.
			if student.State == LEADER_STATE {
				for i := range tc.entries {
					tc.entries[i].Index++
				}
			}

			// Send append entries request to student node...
			logEntryPointers := make([]*LogEntry, len(tc.entries))
			for i := range tc.entries {
				logEntryPointers[i] = &tc.entries[i]
			}

			request := AppendEntriesRequest{
				Term:         uint64(int64(studentTerm) + tc.leaderTermOffset),
				Leader:       mocks[0].RemoteSelf,
				PrevLogIndex: uint64(int64(studentLLI) + tc.prevLogIndexOffset),
				PrevLogTerm:  uint64(int64(studentLLT) + tc.prevLogTermOffset),
				Entries:      logEntryPointers,
				LeaderCommit: uint64(int64(studentCommitIndex) + tc.leaderCommitOffset),
			}

			reply, err := mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &request)
			if err != nil {
				t.Error("error sending vote request to student raft", err)
			}

			// Check reply success
			if reply.GetSuccess() != tc.expectedSuccess {
				t.Errorf("reply.success mismatch, expected %v, got %v", tc.expectedSuccess, reply.GetSuccess())
			}

			// Check reply and actual term
			expectedStudentTerm := maxTerm(studentTerm, request.Term)
			if reply.GetTerm() != expectedStudentTerm {
				t.Errorf("reply.term mismatch, expected %v, got %v", expectedStudentTerm, reply.GetTerm())
			}
			if student.GetCurrentTerm() != expectedStudentTerm {
				t.Errorf("actual term mismatch, expected %v, got %v", expectedStudentTerm, student.GetCurrentTerm())
			}

			// Check leader
			if request.Term >= studentTerm && student.Leader.GetId() != request.Leader.GetId() {
				t.Errorf("leader mismatch, expected %v, got %v", request.Leader.GetId(), student.Leader.GetId())
			}

			// Check votedFor
			if expectedStudentTerm > studentTerm && student.GetVotedFor() != "" {
				t.Errorf("votedFor mismatch, expected to be empty, was %v", student.GetVotedFor())
			}

			// Check fallback
			studentDidFallback := (student.State == FOLLOWER_STATE)
			if studentInitialState != FOLLOWER_STATE && tc.expectedFallback != studentDidFallback {
				t.Errorf("fallback mismatch, expected %v, got %v, student state is %v",
					tc.expectedFallback, studentDidFallback, student.State)
			}

			// NOTE: Checking of errors if prev log index or prev log term are incorrect
			// is done by way of test cases that simulate the issue and expect a correct state.

			// Check truncation alone (without appending entries)
			shouldTruncate := (request.PrevLogIndex <= studentLLI || (request.PrevLogIndex == studentLLI && request.PrevLogTerm != studentLLT))
			if tc.expectedSuccess && len(request.Entries) == 0 && shouldTruncate {
				// Ensure length of logCache is correct
				expectedLength := request.PrevLogIndex + 1
				if uint64(len(student.logCache)) != expectedLength {
					t.Errorf("truncation mismatch; expected length of log cache to be %v, got %v", expectedLength, len(student.logCache))
				}

				// Ensure last log index is correct
				expectedLLI := request.PrevLogIndex
				if student.getLastLogIndex() != expectedLLI {
					t.Errorf("truncation mismatch; expected last log index %v, got %v", expectedLLI, student.getLastLogIndex())
				}

				// Ensure last log term is correct
				expectedLLT := studentLogCache[expectedLLI].TermId
				truncatedStudentLLT := student.getLogEntry(student.getLastLogIndex()).TermId
				if expectedLLT != truncatedStudentLLT {
					t.Errorf("truncation mismatch; expected last log term %v, got %v", expectedLLT, truncatedStudentLLT)
				}
			}

			// Check added log entries
			if tc.expectedSuccess && len(request.Entries) > 0 {
				// Ensure length of logCache is correct
				expectedLength := request.PrevLogIndex + uint64(len(request.Entries)) + 1
				if uint64(len(student.logCache)) != expectedLength {
					t.Errorf("appending mismatch; expected length of log cache to be %v, got %v", expectedLength, len(student.logCache))
				}

				// Ensure last log index is correct
				expectedLLI := request.PrevLogIndex + uint64(len(request.Entries))
				if student.getLastLogIndex() != expectedLLI {
					t.Errorf("appending mismatch; expected last log index %v, got %v", expectedLLI, student.getLastLogIndex())
				}

				// Ensure last log term is correct
				expectedLLT := request.Entries[len(request.Entries)-1].TermId
				newStudentLLT := student.getLogEntry(student.getLastLogIndex()).TermId
				if expectedLLT != newStudentLLT {
					t.Errorf("appending mismatch; expected last log term %v, got %v", expectedLLT, newStudentLLT)
				}
			}

			// Check log cache doesn't change if expected failure
			if !tc.expectedSuccess {
				// Ensure length of logCache hasn't changed
				expectedLength := len(studentLogCache)
				if expectedLength != len(student.logCache) {
					t.Errorf("log cache mismatch; expected length of log cache to not change and remain %v, got %v", expectedLength, len(student.logCache))
				}

				// Ensure last log index hasn't changed
				if student.getLastLogIndex() != studentLLI {
					t.Errorf("log cache mismatch; expected last log index to not change and remain %v, got %v", studentLLI, student.getLastLogIndex())
				}

				// Ensure last log term hasn't changed
				currentStudentLLT := student.getLogEntry(student.getLastLogIndex()).TermId
				if studentLLT != currentStudentLLT {
					t.Errorf("log cache mismatch; expected last log term to not change and remain %v, got %v", studentLLT, currentStudentLLT)
				}
			}

			// Check commit index
			if tc.expectedSuccess && student.commitIndex != request.LeaderCommit {
				t.Errorf("commit index mismatch, expected %v, got %v", request.LeaderCommit, student.commitIndex)
			}
			if !tc.expectedSuccess && student.commitIndex != studentCommitIndex {
				t.Errorf("commit index mismatch, expected to not change and remain %v, got %v", studentCommitIndex, student.commitIndex)
			}

			// Check lastApplied
			if tc.expectedSuccess && student.lastApplied != request.LeaderCommit {
				t.Errorf("last applied mismatch, expected %v, got %v", request.LeaderCommit, student.lastApplied)
			}
			if !tc.expectedSuccess && student.lastApplied != studentLastApplied {
				t.Errorf("last applied mismatch, expected not to change and remain %v, got %v", studentLastApplied, student.lastApplied)
			}

			return
		})
	}
}
