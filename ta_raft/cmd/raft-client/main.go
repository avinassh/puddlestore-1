package main

import (
	"flag"
	"fmt"

	"github.com/abiosoft/ishell"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/client"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/ta_raft/hashmachine"
)

func main() {
	var addr string

	addrHelpString := "Address of an online node of the Raft cluster to connect to."
	flag.StringVar(&addr, "connect", "", addrHelpString)
	flag.StringVar(&addr, "c", "", addrHelpString)

	flag.Parse()

	// Validate address of Raft node
	if addr == "" {
		fmt.Println("Usage: raft-client -c <addr>\nYou must specify an address for the client to connect to!")
		return
	}

	// Connect to Raft node
	client, err := client.Connect(addr)

	if err != nil {
		fmt.Printf("Error starting client: %v\n", err)
		return
	}

	// Kick off shell
	shell := ishell.New()

	shell.AddCmd(&ishell.Cmd{
		Name: "init",
		Help: "initialize the hash machine",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: init <value>")
				return
			}

			err := client.SendRequest(hashmachine.HASH_CHAIN_INIT, []byte(c.Args[0]))
			if err != nil {
				shell.Println(err.Error())
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "hash",
		Help: "perform another round of hashing",
		Func: func(c *ishell.Context) {
			err := client.SendRequest(hashmachine.HASH_CHAIN_ADD, []byte{})
			if err != nil {
				shell.Println(err.Error())
			}
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
