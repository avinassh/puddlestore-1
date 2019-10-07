/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: implements a command line interface for running a Tapestry node.
 */

package main

import (
	"flag"
	"fmt"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/tapestry/tapestry"
	// Uncomment for xtrace
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)

func init() {
	// Uncomment for xtrace
	// err := xtr.Connect(xtr.DefaultServerString)
	// if err != nil {
	// 	fmt.Println("Failed to connect to XTrace server. Ignoring trace logging.")
	// }
	return
}

func main() {
	// Uncomment for xtrace
	// defer xtr.Disconnect()
	var port int
	var addr string
	var debug bool

	flag.IntVar(&port, "port", 0, "The server port to bind to. Defaults to a random port.")
	flag.IntVar(&port, "p", 0, "The server port to bind to. Defaults to a random port. (shorthand)")

	flag.StringVar(&addr, "connect", "", "An existing node to connect to. If left blank, does not attempt to connect to another node.")
	flag.StringVar(&addr, "c", "", "An existing node to connect to. If left blank, does not attempt to connect to another node.  (shorthand)")

	flag.BoolVar(&debug, "debug", false, "Turn on debug message printing.")
	flag.BoolVar(&debug, "d", false, "Turn on debug message printing. (shorthand)")

	flag.Parse()

	tapestry.SetDebug(debug)

	switch {
	case port != 0 && addr != "":
		tapestry.Out.Printf("Starting a node on port %v and connecting to %v\n", port, addr)
	case port != 0:
		tapestry.Out.Printf("Starting a standalone node on port %v\n", port)
	case addr != "":
		tapestry.Out.Printf("Starting a node on a random port and connecting to %v\n", addr)
	default:
		tapestry.Out.Printf("Starting a standalone node on a random port\n")
	}

	t, err := tapestry.Start(port, addr)

	if err != nil {
		fmt.Printf("Error starting tapestry node: %v\n", err)
		return
	}

	tapestry.Out.Printf("Successfully started: %v\n", t)

	// Kick off CLI, await exit
	tapestry.RunTapestryCLI(t)

	tapestry.Out.Println("Closing tapestry")
}
