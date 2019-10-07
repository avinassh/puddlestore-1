package raft

import (
	"testing"
)

func CreateTestingNode(t *testing.T) *RaftNode {
	node, err := CreateNode(0, nil, TestingConfig())
	if err != nil {
		t.Error(err.Error())
		return nil
	}
	return node
}

// Testing helper to append arbitrary elements with the given terms to log
// Doesn't strongly respect internal node state; ONLY USE TO DO LOG COMPARISONS
func (r *RaftNode) AddTermsToLog(terms []uint64) {
	prevTerm := uint64(0)
	for _, term := range terms {
		if term > r.GetCurrentTerm() {
			r.setCurrentTerm(term)
		}

		cmdType := CommandType_STATE_MACHINE_COMMAND
		if term != prevTerm {
			prevTerm = term
			cmdType = CommandType_NOOP
		}

		entry := LogEntry{
			Index:  r.getLastLogIndex() + 1,
			TermId: term,
			Type:   cmdType,
		}
		r.appendLogEntry(entry)
	}
}

func (r *RaftNode) GetTermsFromLog() (terms []uint64) {
	terms = make([]uint64, r.getLastLogIndex()+1)

	for i, entry := range r.getLogEntriesFromCache() {
		terms[i] = entry.TermId
	}
	return
}

func (r *RaftNode) MakeRequestVoteRequestFrom() *RequestVoteRequest {
	lastIndex := r.getLastLogIndex()
	lastTerm := uint64(0)
	if lastIndex >= 0 {
		lastTerm = r.getLogEntry(lastIndex).TermId
	}
	return &RequestVoteRequest{
		Term:         r.GetCurrentTerm(),
		Candidate:    r.GetRemoteSelf(),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
}

func (r *RaftNode) MakeAppendEntriesRequestFrom(prevIndex uint64) *AppendEntriesRequest {
	logEntries := r.getLogEntriesFromCache()
	return &AppendEntriesRequest{
		Term:         r.GetCurrentTerm(),
		Leader:       r.GetRemoteSelf(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  logEntries[prevIndex].GetTermId(),
		Entries:      logEntries[prevIndex+1:], //slice ensures that only elms after prevIndex are sent
		LeaderCommit: r.commitIndex,
	}
}
