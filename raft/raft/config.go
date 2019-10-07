package raft

import (
	"fmt"
	"time"
)

// Config defines the various settings for a Raft cluster.
type Config struct {
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	ClusterSize      int
	NodeIdSize       int
	LogPath          string
	VerboseLog       bool
}

// DefaultConfig returns the default config for a Raft cluster.
func DefaultConfig() *Config {
	config := new(Config)
	config.ClusterSize = 3
	config.ElectionTimeout = time.Millisecond * 150
	config.HeartbeatTimeout = time.Millisecond * 50
	config.NodeIdSize = 2
	config.LogPath = "raftlogs"
	config.VerboseLog = false
	return config
}

func DoubleElectionTimeoutConfig() *Config {
	config := new(Config)
	config.ClusterSize = 3
	config.ElectionTimeout = time.Millisecond * 300
	config.HeartbeatTimeout = time.Millisecond * 50
	config.NodeIdSize = 2
	config.LogPath = "raftlogs"
	config.VerboseLog = false
	return config
}

func TestingConfig() *Config {
	Debug.Output(2, fmt.Sprintf("Created testing config"))
	config := new(Config)
	config.ClusterSize = 3
	config.ElectionTimeout = time.Millisecond * 1500
	config.HeartbeatTimeout = time.Millisecond * 500
	config.NodeIdSize = 2
	config.LogPath = "raftlogs"
	config.VerboseLog = false
	return config
}

func SystemTestingConfig() *Config {
	Debug.Output(2, fmt.Sprintf("Created system testing config"))
	config := new(Config)
	config.ClusterSize = 5
	config.ElectionTimeout = time.Millisecond * 100
	config.HeartbeatTimeout = time.Millisecond * 50
	config.NodeIdSize = 2
	config.LogPath = "raftlogs"
	config.VerboseLog = false
	return config
}

// CheckConfig checks if a provided Raft config is valid.
func CheckConfig(config *Config) error {
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("Heartbeat timeout is too low")
	}

	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("Election timeout is too low")
	}

	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("The election timeout (%v) is less than the heartbeat timeout (%v)", config.ElectionTimeout, config.HeartbeatTimeout)
	}

	if config.ClusterSize <= 0 {
		return fmt.Errorf("The cluster size must be positive")
	}

	if config.NodeIdSize <= 0 {
		return fmt.Errorf("The node id size must be positive")
	}

	if config.LogPath == "" {
		return fmt.Errorf("The log path cannot be empty")
	}

	return nil
}
