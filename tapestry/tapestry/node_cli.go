package tapestry

import (
	"fmt"
	"strings"

	"gopkg.in/abiosoft/ishell.v1"
)

func RunTapestryCLI(t *Node) {
	shell := ishell.New()
	printHelp(shell)

	shell.RegisterGeneric(func(args ...string) (string, error) {
		return fmt.Sprintf("Command not supported: %v\n", args[0]), nil
	})

	shell.Register("quit", func(args ...string) (string, error) {
		t.Leave()
		shell.Println()
		shell.Stop()
		return "", nil
	})

	shell.Register("exit", func(args ...string) (string, error) {
		t.Leave()
		shell.Println()
		shell.Stop()
		return "", nil
	})

	shell.Register("table", func(args ...string) (string, error) {
		return t.RoutingTableToString(), nil
	})

	shell.Register("backpointers", func(args ...string) (string, error) {
		return t.BackpointersToString(), nil
	})

	shell.Register("replicas", func(args ...string) (string, error) {
		return t.LocationMapToString(), nil
	})

	shell.Register("leave", func(args ...string) (string, error) {
		t.Leave()
		return "Left the tapestry", nil
	})

	shell.Register("put", func(args ...string) (string, error) {
		if len(args) > 1 {
			err := t.Store(args[0], []byte(args[1]))
			if err != nil {
				return fmt.Sprintf("Error: %v\n", err.Error()), err
			}
			return fmt.Sprintf("Successfully stored value (%v) at key (%v)", args[1], args[0]), nil
		}

		return "USAGE: put <key> <value>", nil
	})

	shell.Register("list", func(args ...string) (string, error) {
		return t.BlobStoreToString(), nil
	})

	shell.Register("lookup", func(args ...string) (string, error) {
		if len(args) > 0 {
			replicas, err := t.Lookup(args[0])
			if err != nil {
				return fmt.Sprintf("Error!", err.Error()), err
			}
			return fmt.Sprintf("%v: %v\n", args[0], replicas), nil

		}
		return "USAGE: lookup <key>", nil
	})

	shell.Register("get", func(args ...string) (string, error) {
		if len(args) > 0 {
			bytes, err := t.Get(args[0])
			if err != nil {
				return fmt.Sprintf("Error!", err.Error()), err
			}
			return fmt.Sprintf("%v: %v\n", args[0], string(bytes)), nil
		}
		return "USAGE: get <key>", nil
	})

	shell.Register("remove", func(args ...string) (string, error) {
		if len(args) > 0 {
			exists := t.Remove(args[0])
			if !exists {
				return fmt.Sprintf("This node is not advertising %v\n", args[0]), nil
			}
			return fmt.Sprintf("Successfully removed %v\n", args[0]), nil
		}
		return "USAGE: remove <key>", nil
	})

	shell.Register("debug", func(args ...string) (string, error) {
		if len(args) > 0 {
			debugstate := strings.ToLower(args[0])
			switch debugstate {
			case "on", "true":
				{
					SetDebug(true)
					return fmt.Sprintf("Debug turned on"), nil
				}
			case "off", "false":
				{
					SetDebug(false)
					return fmt.Sprintf("Debug turned off"), nil
				}
			default:
				{
					return fmt.Sprintf("Unknown debug state %s. Expect on or off.\n", debugstate), nil
				}
			}
		}
		return "USAGE: debug <on|off>", nil
	})

	shell.Register("help", func(arg ...string) (string, error) {
		printHelp(shell)
		return "", nil
	})

	shell.Register("kill", func(arg ...string) (string, error) {
		t.Kill()
		return "", nil
	})

	shell.Start()
}

func printHelp(shell *ishell.Shell) {
	shell.Println("Commands:")
	shell.Println(" - help                    Prints this help message")
	shell.Println(" - table                   Prints this node's routing table")
	shell.Println(" - backpointers            Prints this node's backpointers")
	shell.Println(" - replicas                Prints the advertised objects that are registered to this node")
	shell.Println("")
	shell.Println(" - put <key> <value>       Stores the provided key-value pair on the local node and advertises the key to the tapestry")
	shell.Println(" - lookup <key>            Looks up the specified key in the tapestry and prints its location")
	shell.Println(" - get <key>               Looks up the specified key in the tapestry, then fetches the value from one of the replicas")
	shell.Println(" - remove <key>            Remove the specified key from the tapestry")
	shell.Println(" - list                    List the blobs being stored and advertised by the local node")
	shell.Println("")
	shell.Println(" - debug on|off            Turn debug on or off.  Off by default")
	shell.Println("")
	shell.Println(" - leave                   Instructs the local node to gracefully leave the tapestry")
	shell.Println(" - kill                    Leaves the tapestry without graceful exit")
	shell.Println(" - exit                    Quit this CLI")
}
