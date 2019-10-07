package main

import (
	"flag"
	"fmt"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/raft/raft"
)

func main() {
	var port int
	var addr string
	var debug bool

	portHelpString := "The server port to bind to. Defaults to a random port."
	flag.IntVar(&port, "port", 0, portHelpString)
	flag.IntVar(&port, "p", 0, portHelpString)

	connectHelpString := "An existing node to connect to. If left blank, does not attempt to connect to another node."
	flag.StringVar(&addr, "connect", "", connectHelpString)
	flag.StringVar(&addr, "c", "", connectHelpString)

	debugHelpString := "Turn on debug message printing."
	flag.BoolVar(&debug, "debug", false, debugHelpString)
	flag.BoolVar(&debug, "d", false, debugHelpString)

	flag.Parse()

	raft.SetDebug(debug)

	// Initialize Raft with testing config
	config := raft.TestingConfig()

	// Parse address of remote Raft node
	var remoteNode *raft.RemoteNode
	if addr != "" {
		remoteNode = &raft.RemoteNode{Id: raft.AddrToId(addr, config.NodeIdSize), Addr: addr}
	}

	// Create Raft node
	fmt.Println("Starting a Raft node...")
	raftNode, err := raft.CreateNode(port, remoteNode, config)

	if err != nil {
		fmt.Printf("Error starting Raft node: %v\n", err)
		return
	}

	fmt.Printf("Successfully created Raft node: %v\n", raftNode)

	raft.RunRaftShell(raftNode)
}
