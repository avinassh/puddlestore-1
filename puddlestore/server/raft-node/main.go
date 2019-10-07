package main

import (
	"flag"
	"fmt"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/puddlestore"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/raft"
)

func main() {
	var raftPort int
	var zkaddr string
	var debug bool
	var noshell bool

	connectHelpString := "The list of PuddleStore Zookeeper server addresses to connect to"
	flag.StringVar(&zkaddr, "connect", puddlestore.DEFAULT_ZK_ADDR, connectHelpString)
	flag.StringVar(&zkaddr, "c", puddlestore.DEFAULT_ZK_ADDR, connectHelpString)

	portHelpString := "The raft node's port to bind to. Defaults to a random port."
	flag.IntVar(&raftPort, "port", 0, portHelpString)
	flag.IntVar(&raftPort, "p", 0, portHelpString)

	debugHelpString := "Turn on debug message printing."
	flag.BoolVar(&debug, "debug", false, debugHelpString)
	flag.BoolVar(&debug, "d", false, debugHelpString)

	shellHelpString := "Disables interactive shell."
	flag.BoolVar(&noshell, "noshell", false, shellHelpString)
	flag.BoolVar(&noshell, "ns", false, shellHelpString)

	flag.Parse()

	// Validate address of Zookeeper servers
	if zkaddr == "" {
		fmt.Println("Usage: raft-node -c <servers> \nYou must specify a list of Zookeeper server addresses to connect to.")
		return
	}

	fmt.Println("Starting a Raft node...")
	raftNode, conn, err := puddlestore.CreatePuddleStoreRaftNode(zkaddr, raftPort, debug)
	if err != nil {
		fmt.Printf("Error starting Raft node: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Printf("Successfully created Raft node: %v\n", raftNode)

	// Run normal raft interface shell if configured to
	if noshell {
		// hang until user exits
		select {}
	} else {
		raft.RunRaftCLI(raftNode)
	}
	fmt.Println("Closing Raft node")
}
