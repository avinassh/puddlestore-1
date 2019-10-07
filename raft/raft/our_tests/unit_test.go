package our_tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldVoteForTerms(t *testing.T) {
	node1 := CreateTestingNode(t)
	node2 := CreateTestingNode(t)

	node1.AddTermsToLog([]uint64{1, 1, 2})
	node2.AddTermsToLog([]uint64{1, 1})

	// higher term requesting from lower term
	node1.setCurrentTerm(2)
	node2.setCurrentTerm(1)
	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	// lower term requesting from higher term
	node1.setCurrentTerm(2)
	node2.setCurrentTerm(3)
	assert.False(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))
}

func TestShouldVoteForVotedFor(t *testing.T) {
	node1 := CreateTestingNode(t)
	node2 := CreateTestingNode(t)

	node1.AddTermsToLog([]uint64{1, 1, 2, 3, 3})
	node2.AddTermsToLog([]uint64{1, 1, 2, 3})
	node1.setCurrentTerm(3)
	node2.setCurrentTerm(3)

	// already voted
	node2.setVotedFor("lala")
	assert.False(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	// already voted, but voted for node1 (log is more up to date)
	node2.setVotedFor(node1.GetRemoteSelf().GetAddr())
	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	// hasn't voted, log is more up to date
	node2.setVotedFor("")
	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))
}

func TestShouldVoteForVotedLogs(t *testing.T) {
	node1 := CreateTestingNode(t)
	node2 := CreateTestingNode(t)

	node1.AddTermsToLog([]uint64{0})
	node2.AddTermsToLog([]uint64{0})
	node1.setCurrentTerm(0)
	node2.setCurrentTerm(0)
	node1.setVotedFor("")
	node2.setVotedFor("")

	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	node1.AddTermsToLog([]uint64{0})
	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	node2.AddTermsToLog([]uint64{0, 0, 0})
	assert.False(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	node1.setCurrentTerm(1)
	node2.setCurrentTerm(1)
	node1.AddTermsToLog([]uint64{1})
	node1.AddTermsToLog([]uint64{1})
	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	node2.AddTermsToLog([]uint64{1})
	assert.False(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))

	node1.setCurrentTerm(2)
	node1.AddTermsToLog([]uint64{2, 2})
	assert.True(t, node2.shouldVoteFor(node1.MakeRequestVoteRequestFrom()))
}

func TestMergeLogEntriesEdgeCases(t *testing.T) {
	node1 := CreateTestingNode(t)
	node2 := CreateTestingNode(t)

	node1.AddTermsToLog([]uint64{0, 0, 1, 1})
	node2.AddTermsToLog([]uint64{0, 0, 1})
	node1.setCurrentTerm(1)
	node2.setCurrentTerm(1)

	// always succeeds if nil or no entries
	assert.True(t, node2.mergeLogEntries(nil))
	msg := AppendEntriesRequest{
		Term:         node1.GetCurrentTerm(),
		Leader:       node1.GetRemoteSelf(),
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
	}
	assert.True(t, node2.mergeLogEntries(&msg))

	// bad prevLogIndex
	msg = AppendEntriesRequest{
		Term:         node1.GetCurrentTerm(),
		Leader:       node1.GetRemoteSelf(),
		PrevLogIndex: 4,
		PrevLogTerm:  1,
		Entries:      node1.getLogEntriesFromCache(),
		LeaderCommit: 0,
	}
	assert.False(t, node2.mergeLogEntries(&msg))
}

func TestMergeLogEntriesNormal(t *testing.T) {
	node1 := CreateTestingNode(t)
	node2 := CreateTestingNode(t)

	node1.AddTermsToLog([]uint64{0, 0, 1, 1}) // + initial 0 term
	node2.AddTermsToLog([]uint64{0, 0, 1})    // + initial 0 term
	node1.setCurrentTerm(1)
	node2.setCurrentTerm(1)
	node1.commitIndex = 2
	node2.commitIndex = 0

	// adding one element on
	assert.True(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-1)))
	assert.Equal(t, node2.GetTermsFromLog(), node1.GetTermsFromLog())
	assert.Equal(t, node1.commitIndex, node2.commitIndex)

	// no adding
	assert.True(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-1)))
	assert.Equal(t, node2.GetTermsFromLog(), node1.GetTermsFromLog())
	assert.Equal(t, node1.commitIndex, node2.commitIndex)

	// bad prevIndex (too high)
	prevTerms := node2.GetTermsFromLog()
	node1.AddTermsToLog([]uint64{1, 1})
	node1.commitIndex = node1.getLastLogIndex() - 1
	assert.NotEqual(t, node1.commitIndex, node2.commitIndex)
	assert.False(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-1)))
	assert.Equal(t, prevTerms, node2.GetTermsFromLog())
	assert.NotEqual(t, node1.commitIndex, node2.commitIndex)

	// many items, with slow decrease to prev index
	node1.AddTermsToLog([]uint64{1, 2, 2})
	node1.setCurrentTerm(2)
	node1.setCurrentTerm(2)
	assert.False(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-1)))
	assert.Equal(t, prevTerms, node2.GetTermsFromLog())
	assert.NotEqual(t, node1.commitIndex, node2.commitIndex)
	assert.False(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-2)))
	assert.Equal(t, prevTerms, node2.GetTermsFromLog())
	assert.NotEqual(t, node1.commitIndex, node2.commitIndex)
	assert.False(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-3)))
	assert.Equal(t, prevTerms, node2.GetTermsFromLog())
	assert.NotEqual(t, node1.commitIndex, node2.commitIndex)
	assert.False(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-4)))
	assert.Equal(t, prevTerms, node2.GetTermsFromLog())
	assert.NotEqual(t, node1.commitIndex, node2.commitIndex)
	assert.True(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-5)))
	assert.Equal(t, node2.GetTermsFromLog(), node1.GetTermsFromLog())
	assert.Equal(t, node1.commitIndex, node2.commitIndex)

	// 2 needs to remove some
	node1.setCurrentTerm(3)
	node1.setCurrentTerm(3)
	node1.AddTermsToLog([]uint64{2, 3})
	node1.commitIndex += 2
	node2.AddTermsToLog([]uint64{2, 2, 2, 2, 3})
	node2.commitIndex++
	assert.True(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-1)))
	assert.Equal(t, node2.GetTermsFromLog(), node1.GetTermsFromLog())
	assert.Equal(t, node1.commitIndex, node2.commitIndex)

	node1.AddTermsToLog([]uint64{3, 3})
	node2.AddTermsToLog([]uint64{3, 3, 3, 3, 3})
	assert.True(t, node2.mergeLogEntries(node1.MakeAppendEntriesRequestFrom(node1.getLastLogIndex()-4)))
	assert.Equal(t, node2.GetTermsFromLog(), node1.GetTermsFromLog())
	assert.Equal(t, node1.commitIndex, node2.commitIndex)
}
