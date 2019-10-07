#!/usr/bin/env bash

DEBUG=${1:-"false"} # default off
TAPESTRY_SIZE=${2:-5} # default to one size but otherwise take first arg

ARGS="-c localhost:2181 -ns"
#if [ -n DEBUG ]
#then
#    echo "debugging on"
#    ARGS="$ARGS -d"
#fi

function quit {
    echo "\nstopping zookeeper"
    sudo /opt/zookeeper-3.4.12/bin/zkServer.sh stop > /dev/null
    echo "stopping all nodes"
    pkill -2 main
}

echo "usage: launch-test-puddle.bash <num tapestry nodes>"

# cleanup
rm -rf ./raftlogs/
rm -rf ./logs/*

# clean ports
#echo "cleaning ports..."
#sudo ifconfig lo down
#sleep 5
#sudo ifconfig lo up
#sleep 5

# start local Zookeeper server
log= "./logs/zookeeper_server.log"
echo "Launching zookeeper server, logging to $log"
sudo /opt/zookeeper-3.4.12/bin/zkServer.sh start & > $log

# kill extra processes on exit
trap "quit; exit 0;" SIGINT

# start raft cluster connected to Zookeeper
for i in 0 1 2
do
    log="./logs/raft_node_$i.log"
    echo "Launching raft node $i, logging to $log"
    go run server/raft-node/main.go $ARGS &> $log &
    sleep 1
done

# start tapestry nodes connected to Zookeeper, based on specified amount
for (( i=0; i<TAPESTRY_SIZE; i++ ))
do
    log="./logs/tapestry_node_$i.log"
    echo "Launching tapestry node $i, logging to $log"
    go run server/tapestry-node/main.go $ARGS &> $log &
    sleep 1
done

## start CLI to puddlestore system
echo "Waiting for everything to initialize..."
#sleep 5
#echo "Launching CLI application..."
#go run client/puddle-cli.go -c localhost:2181

# handle exit
echo "Press Ctrl-C to close everything"
sleep infinity
quit