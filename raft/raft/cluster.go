package raft

// CreateLocalCluster creates a new Raft cluster with the given config in the
// current process.
func CreateLocalCluster(config *Config) ([]*RaftNode, error) {
	if config == nil {
		config = DefaultConfig()
	}
	err := CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*RaftNode, config.ClusterSize)

	nodes[0], err = CreateNode(0, nil, config)
	if err != nil {
		Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		nodes[i], err = CreateNode(0, nodes[0].GetRemoteSelf(), config)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// CreateDefinedLocalCluster creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalCluster(config *Config, ports []int) ([]*RaftNode, error) {
	if config == nil {
		config = DefaultConfig()
	}
	err := CheckConfig(config)
	if err != nil {
		return nil, err
	}

	nodes := make([]*RaftNode, config.ClusterSize)

	nodes[0], err = CreateNode(ports[0], nil, config)
	if err != nil {
		Error.Printf("Error creating first node: %v", err)
		return nodes, err
	}

	for i := 1; i < config.ClusterSize; i++ {
		nodes[i], err = CreateNode(ports[i], nodes[0].GetRemoteSelf(), config)
		if err != nil {
			Error.Printf("Error creating %v-th node: %v", i, err)
			return nil, err
		}
	}

	return nodes, nil
}
