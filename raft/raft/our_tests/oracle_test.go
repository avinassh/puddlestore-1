package our_tests

import (
	"testing"
	"time"
)

const RUN_ORACLES = false
const BTWN_CMD_DELAY = 250 * time.Millisecond

func (rc *RaftCluster) RandomTestSequence(length int, delay time.Duration, allowExits bool, allowPartitions bool) {
	// start at least one client
	rc.ClusterCmdClientStart()
	for i := 0; i < length; i++ {
		rc.Printf("running command %d/%d\n\n", i, length)
		rc.RandomClusterCmd(length, allowExits, allowPartitions)
		rc.assertNodeLogs()
		<-time.NewTimer(delay).C
		rc.assertNodeLogs()
		rc.assertLeaderState()
	}
}

// runs a random command with a reasonable set of probabilities
func (rc *RaftCluster) RandomClusterCmd(length int, allowExits bool, allowPartitions bool) {
	if withProb(0.1) {
		// make sure we don't hang the whole test on a cluster that can't fulfill a client
		// request
		go rc.ClusterCmdClientRegister(nil)
	}
	if withProb(0.25) {
		// make sure we don't hang the whole test on a cluster that can't fulfill a client
		// request
		go rc.ClusterCmdClientCmdRand()
	}
	if withProb(0.05) {
		rc.ClusterCmdClientLeave()
	}

	if allowExits {
		if withProb(0.05) {
			rc.ClusterCmdNodeShutdown(nil)
		}
		if withProb(0.05) {
			rc.ClusterCmdNodeRestart(nil)
		}
	}

	if allowPartitions {
		if withProb(0.04) {
			rc.ClusterCmdRandNodeSever()
		}
		if withProb(0.05) {
			rc.ClusterCmdRandPartitionRand()
		}
		if withProb(0.05) {
			rc.ClusterCmdRandPartitionSplitRand()
		}
		if withProb(0.4) {
			rc.ClusterCmdResolvePartition()
		}
	}
}

func TestRandomSequenceNormal(t *testing.T) {
	if RUN_ORACLES {
		rc, err := NewRaftCluster(t, SystemTestingConfig())
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		rc.RandomTestSequence(100, BTWN_CMD_DELAY, false, false)
	}
}

func TestRandomSequenceExits(t *testing.T) {
	if RUN_ORACLES {
		rc, err := NewRaftCluster(t, SystemTestingConfig())
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		rc.strictTests = false // too many failures

		rc.RandomTestSequence(100, BTWN_CMD_DELAY, true, false)
	}
}

func TestRandomSequencePartitions(t *testing.T) {
	if RUN_ORACLES {
		rc, err := NewRaftCluster(t, SystemTestingConfig())
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		rc.strictTests = false // too many failures

		rc.RandomTestSequence(100, BTWN_CMD_DELAY, false, true)
	}
}
