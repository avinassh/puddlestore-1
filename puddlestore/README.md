# PuddleStore
## Implementation Points
- We chose to only use AGUID->VGUID mappings for INOde blocks (Files and Directories), to put more stress
  on the larger Tapestry cluster instead of the smaller inner ring of Raft servers.
- Our external API is much more object-oriented, where the objects are references to Files or Directories,
  than it is path-oriented. We still have an open method that will route to a path, but it is
  rarely used in our shell client. In general we designed our API to be used in settings where
  operations are done in a particular directory, much like a user would use a shell.

## How to use

### Installing zookeeper:
Run `./install-zookeeper.bash` in the root of the repository.

OR:
1) Download from this link: http://mirrors.ibiblio.org/apache/zookeeper/stable/
2) unzip to some installation directory "dir"
3) create "dir"/conf/zoo.cfg file with the following contents:
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/var/zookeeper/data
clientPort=2181
4) run "dir"/bin/zkServer.sh (should start up)
5) run "dir"/bin/zkCli.sh (should connect)
6) type ls / and [zookeeper] should show up

### Running
From the puddlestore root, run:
```
./launch-test-puddle.bash
```

Alternatively (and even from many computers) you can start up a full cluster with:
```
../zookeeper/bin/zkServer.sh start-foreground &             # start local Zookeeper server
go run server/raft-node/main.go     # start raft node connected to Zookeeper (do this THREE times)
go run server/tapestry-node/main.go # start tapestry node connected to Zookeeper (do this as many times as you want; 5 is good)
go run client/puddle-cli.go         # start CLI to puddlestore system
```

### Running Tests
Our tests don't auto-start Zookeeper, so you'll need to run the server at localhost:2181.
Assuming you installed Zookeeper as our script did, then from the puddlestore root, run:
```
./zookeeper/bin/zkServer.sh start
```
Then, you can proceed to run `go test` or use an IDE tester.

## Testing Overview
NOTE: Tests will likely not work when run concurrently! (raftlogs)
#### Unit Tests
- Inode
    - encode/decode
- Dblock
    - encode/decode
- Path
    - constructor
    - List/Append/Split
- File
    - Read
    - Write
- More (see tests)

#### System Tests
- Server
    - GetTapestryClient
    - GetRaftClient
- Block Storage
    - addMappingToRaft
    - addObjectToTapestry
- File System Generator
    - fs_generator_test.go
    - Cd/Mkdir/Touch/Edit/Rm
- General System/Client Tests
    - client_test.go
- Multi Client Tests
    - Basic write on one client, read on another test
    - Cd into directory on one client, removal of that dir on another, original can cd out

