package main

import (
	"flag"
	"fmt"

	"io/ioutil"
	"os"
	"os/exec"

	"strconv"

	"strings"

	"github.com/abiosoft/ishell"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/client"
	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/puddlestore"
	"github.com/gobuffalo/uuid"
)

const DEFAULT_EDITOR_CMD = "vim"

/************************************************************************************/
// Client Command System
/************************************************************************************/
func main() {
	var servers string

	addrHelpString := "The list of PuddleStore Zookeeper server addresses to connect to."
	flag.StringVar(&servers, "connect", puddlestore.DEFAULT_ZK_ADDR, addrHelpString)
	flag.StringVar(&servers, "c", puddlestore.DEFAULT_ZK_ADDR, addrHelpString)
	flag.Parse()

	// Validate address of Zookeeper servers
	if servers == "" {
		fmt.Println("Usage: puddle-cli -c <servers> \nYou must specify a list of Zookeeper server addresses to connect to.")
		return
	}

	// Connect to Tapestry and get root node
	fmt.Printf("Connecting to puddlestore at %s\n", servers)
	state, err := client.NewFileSystem(servers)
	if err != nil {
		fmt.Printf("Error connecting to puddlestore: %v\n", err)
		return
	}
	defer state.Close()
	createShell(state)
}

func greenStr(str string) string {
	return fmt.Sprintf("\033[1;32m%s\033[0m", str)
}

func blueStr(str string) string {
	return fmt.Sprintf("\033[1;34m%s\033[0m", str)
}

func getPrompt(state *client.FileSystemState) string {
	return blueStr(state.Path.String()) + greenStr(" $ ")
}

func shortUID(uuid string) string {
	if len(uuid) < 8 {
		return uuid
	}
	return uuid[:8]
}

/*
	Starts shell and works with state to allow user to navigate filesystem
*/
func createShell(state *client.FileSystemState) {
	// Kick off shell
	shell := ishell.New()

	shell.SetPrompt(getPrompt(state))

	shell.AddCmd(&ishell.Cmd{
		Name: "ls",
		Help: "list the contents of the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) > 1 || len(c.Args) == 1 && c.Args[0] != "-l" {
				shell.Println("Usage: ls [-l]")
				return
			}
			longFormat := len(c.Args) == 1 && c.Args[0] == "-l"

			err := state.ReloadCurrentDir()
			if err != nil {
				shell.Printf("ls: error reloading directory: %v\n", err.Error())
				return
			}

			inodes, err := state.CurrentDir.Contents()
			if err != nil {
				shell.Printf("ls: error listing contents: %v\n", err)
				return
			}
			for _, inode := range inodes {
				name := inode.GetName()
				sizeSuffix := " bytes"
				if inode.IsDirectory() {
					name = blueStr(name)
					sizeSuffix = " items"
				}
				if longFormat {
					blockList := make([]string, len(inode.Blocks))
					for i, blockUID := range inode.Blocks {
						blockList[i] = shortUID(blockUID)
					}

					shell.Printf("%s\t%5d%s  %s -> %s : %v\n", name, inode.GetSize(), sizeSuffix,
						shortUID(inode.GetAGUID()), shortUID(inode.GetVGUID()), blockList)
				} else {
					shell.Printf("%s\t", name)
				}
			}
			if len(inodes) > 0 && !longFormat {
				shell.Println("")
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "cd",
		Help: "change the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: cd <absolute path or relative sub-path>|..")
				return
			}
			_, err := state.Cd(c.Args[0])
			if err != nil {
				shell.Printf("cd: error changing directory: %v\n", err)
				return
			}
			shell.SetPrompt(getPrompt(state))
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "pwd",
		Help: "print the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 0 {
				shell.Println("Usage: pwd")
				return
			}
			shell.Println(state.Path.String())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "mkdir",
		Help: "create a directory in the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: mkdir <subdirectory name>")
				return
			}
			_, err := state.Mkdir(c.Args[0])
			if err != nil {
				shell.Printf("mkdir: error creating directory: %v\n", err)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "touch",
		Help: "create a file in the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: touch <filename>")
				return
			}
			_, err := state.Touch(c.Args[0])
			if err != nil {
				shell.Printf("touch: error creating file: %v\n", err)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "rm",
		Help: "remove a file or directory in the current directory, recursively",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: rm <inode or subdirectory name>")
				return
			}

			err := state.Rm(c.Args[0])
			if err != nil {
				shell.Printf("rm: %v\n", err)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "cat",
		Help: "print the contents of a file in the current directory",
		Func: func(c *ishell.Context) {
			usage := "Usage: cat <filename> [start offset] [amount]"
			if len(c.Args) < 1 || len(c.Args) > 3 {
				shell.Println(usage)
				return
			}

			start := 0
			amount := 0 // full file
			var err error
			if len(c.Args) >= 2 {
				start, err = strconv.Atoi(c.Args[1])
				if err != nil {
					shell.Printf("error parsing start offset: %v\n%s\n", err, usage)
					return
				}
			}
			if len(c.Args) >= 3 {
				amount, err = strconv.Atoi(c.Args[2])
				if err != nil {
					shell.Printf("error parsing amount: %v\n%s\n", err, usage)
				}
			}

			out, err := state.CatN(c.Args[0], uint64(start), uint64(amount))
			if err != nil {
				shell.Printf("cat: %v\n", err)
				return
			}
			// dump bytes to screen
			if len(out) > 0 {
				shell.Print(out)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "edit",
		Help: "edit a file in the current directory using the default or specified text editor",
		Func: func(c *ishell.Context) {
			usage := "Usage: edit <filename> [start offset] [command]"
			if len(c.Args) < 1 || len(c.Args) > 3 {
				shell.Println(usage)
				return
			}
			start := -1
			command := DEFAULT_EDITOR_CMD
			var err error
			if len(c.Args) >= 2 {
				start, err = strconv.Atoi(c.Args[1])
				if err != nil {
					shell.Printf("error parsing start offset: %v\n%s\n", err, usage)
					return
				}
			}
			if len(c.Args) >= 3 {
				command = c.Args[2]
				if strings.ContainsAny(command, " \t;") {
					shell.Printf("command cannot have arguments: %v\n", err, usage)
					return
				}
			}

			file, found, err := state.GetChildFile(c.Args[0])
			if err != nil {
				shell.Printf("edit: %v\n", err)
				return
			}
			// create file if not found
			if !found {
				file, err = state.Touch(c.Args[0])
				if err != nil {
					shell.Printf("edit: error creating file: %v\n", err)
					return
				}
			}
			// open file in text editor then write changes back
			err = UserEditINode(file, start, command)
			if err != nil {
				shell.Printf("edit: %v\n", err)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "scp",
		Help: "move a local file into puddlestore",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 2 {
				shell.Println("Usage: scp <local> <remote>") // [command]")
				return
			}

			_, err := state.Scp(c.Args[0], c.Args[1])
			if err != nil {
				shell.Printf("scp: %v\n", err)
				return
			}
		},
	})

	shell.Println(`
Welcome to
__________          .___  .___.__           _________ __
\______   \__ __  __| _/__| _/|  |   ____  /   _____//  |_  ___________  ____
|     ___/  |  \/ __ |/ __ | |  | _/ __ \ \_____  \\   __\/  _ \_  __ \/ __ \
|    |   |  |  / /_/ / /_/ | |  |_\  ___/ /        \|  | (  <_> )  | \|  ___/
|____|   |____/\____ \____ | |____/\___  >_______  /|__|  \____/|__|   \___  >
\/    \/           \/        \/                        \/

Enter 'help' for a list of commands`)

	shell.Run()
}

/*
	Opens a text editor for a user to edit an INode by creating a temp file.
	Makes sure to populate the file with the current contents for the user to view,
	and writes the whole file (all changes) back when finished.
*/
func UserEditINode(file *puddlestore.File, start int, editorCommand string) error {
	// generate random filename for tmp file
	randStr, _ := uuid.NewV4()
	randFileName := "." + file.GetName() + "-" + randStr.String() + ".tmp"

	// read contents from PuddleStore and write to local temp file
	var curBytes []byte
	var err error
	if start < 0 {
		curBytes, err = file.ReadFrom(0)
	} else {
		curBytes, err = file.ReadFrom(uint64(start))
	}
	if err != nil {
		return fmt.Errorf("error reading remote file: %v ", err)
	}
	err = ioutil.WriteFile(randFileName, curBytes, 0644)
	if err != nil {
		return fmt.Errorf("error writing local file: %v ", err)
	}

	// launch editor and let user edit the file
	cmnd := exec.Command(editorCommand, randFileName)
	cmnd.Stdout = os.Stdout
	cmnd.Stdin = os.Stdin
	err = cmnd.Run() // and wait
	if err != nil {
		return fmt.Errorf("error launching editor: %v ", err)
	}

	// read the modified curBytes from the file and write them back to puddlestore
	newBytes, err := ioutil.ReadFile(randFileName)
	if err != nil {
		return fmt.Errorf("error reading local file: %v ", err)
	}
	// either overwrite contents or write to specific location
	if start < 0 {
		err = file.Write(newBytes)
	} else {
		err = file.WriteTo(uint64(start), newBytes)
	}
	if err != nil {
		return fmt.Errorf("error writing remote file: %v ", err)
	}
	err = os.Remove(randFileName)
	if err != nil {
		return fmt.Errorf("error removing local file: %v", err)
	}
	fmt.Printf("wrote %d bytes\n", len(newBytes))
	return nil
}
