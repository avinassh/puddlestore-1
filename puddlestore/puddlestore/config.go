package puddlestore

import (
	"time"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/raft"
)

const DBLOCKSIZE = 4096 //TESTING; was 4096 //block Size in bytes
const PATH_SEPERATOR = "/"
const DEFAULT_ZK_ADDR = "localhost:2181"
const MIN_TAPESTRY_NODES = 1
const INVALID_PATH_CHARS = PATH_SEPERATOR

func PuddleStoreRaftConfig() *raft.Config {
	config := new(raft.Config)
	config.ClusterSize = 3
	config.ElectionTimeout = time.Millisecond * 450
	config.HeartbeatTimeout = time.Millisecond * 150
	config.NodeIdSize = 2
	config.LogPath = "raftlogs"
	//config.VerboseLog = false
	return config
}
