package raft

import (
	"os/user"
	"runtime"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	suppressLoggers()
	t.Run("follower init", testFollowerInit)
	t.Run("candidate formed", testCandidateFormed)
	t.Run("leader elected", testLeaderElected)

	config := DefaultConfig()
	if runtime.GOOS != "windows" {
		curUser, _ := user.Current()
		config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	}

	t.Run("default cluster init", func(t *testing.T) {
		testClusterInit(t, config)
	})

	config.ClusterSize = 5
	t.Run("different cluster size init", func(t *testing.T) {
		testClusterInit(t, config)
	})
}

func testFollowerInit(t *testing.T) {
	student, mocks, err := createFollowerMockCluster(t)
	defer cleanupMockCluster(student, mocks)
	if err != nil {
		t.Fatal(err)
	}
}

func testCandidateFormed(t *testing.T) {
	student, mocks, err := createCandidateMockCluster(t)
	defer cleanupMockCluster(student, mocks)
	if err != nil {
		t.Fatal(err)
	}
}

func testLeaderElected(t *testing.T) {
	student, mocks, err := createLeaderMockCluster(t)
	defer cleanupMockCluster(student, mocks)
	if err != nil {
		t.Fatal(err)
	}
}

func testClusterInit(t *testing.T, config *Config) {
	cluster, err := CreateLocalCluster(config)
	defer cleanupStudentCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(8 * config.ElectionTimeout)

	followerCount := 0
	candidateCount := 0
	leaderCount := 0

	for i := 0; i < config.ClusterSize; i++ {
		node := cluster[i]
		switch node.State {
		case FOLLOWER_STATE:
			followerCount++
		case CANDIDATE_STATE:
			candidateCount++
		case LEADER_STATE:
			leaderCount++
		}
	}

	if followerCount != config.ClusterSize-1 {
		t.Errorf("follower count mismatch, expected %v, got %v", config.ClusterSize-1, followerCount)
	}

	if candidateCount != 0 {
		t.Errorf("candidate count mismatch, expected %v, got %v", 0, candidateCount)
	}

	if leaderCount != 1 {
		t.Errorf("leader count mismatch, expected %v, got %v", 1, leaderCount)
	}
}
