#/usr/bin/env bash


STARTING_PORT=1234

echo "Spawning first node"
rm -r raftlogs/
#gnome-terminal -- ./tapestry-ta -d -p $STARTING_PORT &
gnome-terminal -- go run cmd/raft-node/main.go -d -p $STARTING_PORT &

node_port=$STARTING_PORT
#while [ 1 ]
#do
    #read -p "Spawn node? [enter]"
    #node_port=$(($node_port+1))
    gnome-terminal -- go run cmd/raft-node/main.go -d -c 127.0.1.1:$STARTING_PORT &
    sleep 1
    gnome-terminal -- go run cmd/raft-node/main.go -d -c 127.0.1.1:$STARTING_PORT &
    sleep 1
    #gnome-terminal -- go run cmd/raft-node/main.go -d -c 127.0.1.1:$STARTING_PORT &
    #sleep 1

#done

echo "Starting client"
go run cmd/raft-client/main.go -c 127.0.1.1:$STARTING_PORT
