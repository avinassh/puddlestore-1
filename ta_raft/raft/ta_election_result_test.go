package raft

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"
)

type electionTestCase struct {
	title         string
	votes         []Vote
	expectedState NodeState
}

func votePermutations(arr []Vote) [][]Vote {
	var helper func([]Vote, int)
	res := [][]Vote{}

	helper = func(arr []Vote, n int) {
		if n == 1 {
			tmp := make([]Vote, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					tmp := arr[i]
					arr[i] = arr[n-1]
					arr[n-1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n-1]
					arr[n-1] = tmp
				}
			}
		}
	}
	helper(arr, len(arr))
	return res
}

type VoteArray [][]Vote

func (va VoteArray) Len() int {
	return len(va)
}

func (va VoteArray) Swap(i, j int) {
	va[i], va[j] = va[j], va[i]
}

func (va VoteArray) Less(i, j int) bool {
	for k := 0; k < len(va[i]); k += 1 {
		if va[i][k] != va[j][k] {
			return va[i][k] < va[j][k]
		}
	}

	return false
}

func uniqueVotes(votes [][]Vote) [][]Vote {
	sort.Sort(VoteArray(votes))
	output := make([][]Vote, 0, len(votes))
	for i := 0; i < len(votes)-1; i += 1 {
		eq := true
		for j := 0; j < len(votes[i]); j++ {
			if votes[i][j] != votes[i+1][j] {
				eq = false
			}
		}
		if !eq {
			output = append(output, votes[i+1])
		}
	}
	return output
}

func generateAllCases(clusterSize int) [][]Vote {
	possible := []Vote{YES, NO, ERROR, HANG}

	var allPossible func(i int, soFar []Vote, possible []Vote) [][]Vote

	allPossible = func(i int, soFar []Vote, possible []Vote) [][]Vote {
		retvals := [][]Vote{}

		if len(possible) == 1 {
			newPart := make([]Vote, i)
			for k, _ := range newPart {
				newPart[k] = possible[0]
			}
			return [][]Vote{append(soFar, newPart...)}
		}
		for j := 0; j <= i; j += 1 {
			newPart := make([]Vote, j)
			for k, _ := range newPart {
				newPart[k] = possible[0]
			}
			retvals = append(retvals, allPossible(i-j, append(soFar, newPart...), possible[1:])...)
		}

		return retvals
	}

	possibleVotes := allPossible(clusterSize-1, []Vote{}, possible)
	voteCases := make([][]Vote, 0)

	for _, votes := range possibleVotes {
		voteCases = append(voteCases, votePermutations(votes)...)
	}

	return uniqueVotes(voteCases)
}

func TestElectionResults(t *testing.T) {
	suppressLoggers()

	setup := func(size int, t *testing.T) (student *RaftNode, mocks []*MockRaft) {
		// Create mock cluster with one student node, and mocks that always vote yes
		config := DefaultConfig()
		config.ElectionTimeout = 500 * time.Millisecond
		config.ClusterSize = size
		student, mocks, err := MockCluster(false, config, t)
		if err != nil {
			t.Error("Couldn't set up mock cluster", err)
		}

		for i := 0; i < 3; i++ {
			student.appendLogEntry(LogEntry{
				TermId: uint64(i + 1),
				Index:  uint64(i),
				Type:   CommandType_NOOP,
				Data:   []byte{},
			})
		}

		student.setCurrentTerm(3)

		return
	}

	title := func(votes []Vote) string {
		var buf bytes.Buffer

		for i, v := range votes {
			fmt.Fprint(&buf, v)
			if i != len(votes)-1 {
				fmt.Fprint(&buf, " ")
			}
		}

		return buf.String()
	}

	count := func(votes []Vote, target Vote) (num int) {
		for _, v := range votes {
			if v == target {
				num += 1
			}
		}

		return
	}

	isMajority := func(votes []Vote) bool {
		yeses := count(votes, YES)

		return yeses >= (len(votes) / 2)
	}

	// run every group of 25 in parallel
	inParallel := 25

	grpcTimeout := 2*time.Second + 100*time.Millisecond

	runCases := func(size int, t *testing.T) {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cases := generateAllCases(size)
			t.Log("running all", len(cases), "cases for cluster size", size)
			for i, tc := range cases {
				tc := tc
				t.Run(title(tc), func(t *testing.T) {
					if i%inParallel != 0 {
						t.Parallel()
					}

					clusterSize := len(tc) + 1
					student, mocks := setup(clusterSize, t)

					defer cleanupMockCluster(student, mocks)

					for i, m := range mocks {
						_ = m.setVoteHandler(tc[i])
						err := m.JoinCluster()
						if err != nil {
							t.Fatal("error on join", err)
						}
					}

					time.Sleep(2*student.config.ElectionTimeout + 200*time.Millisecond)

					for sleep := 0; sleep < count(tc, HANG); sleep += 1 {
						time.Sleep(grpcTimeout)
					}

					if isMajority(tc) {
						if student.State != LEADER_STATE {
							t.Error("didn't become leader, is:", student.State)
						} else {
							e := student.getLogEntry(student.getLastLogIndex())
							if e.GetTermId() != student.GetCurrentTerm() ||
								e.GetType() != CommandType_NOOP {
								t.Error("student didn't append noop of current term")
							}
						}
					} else {
						if student.State == LEADER_STATE {
							t.Error("became leader", tc)
						}
					}

				})
			}
		})
	}

	runCases(3, t)
	runCases(5, t)

}
