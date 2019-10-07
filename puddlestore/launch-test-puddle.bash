#!/usr/bin/env bash

INIT_WAIT_S=0
ZOOKEEPER_ROOT=../zookeeper

DEBUG=${1:-0} # default off
TAPESTRY_SIZE=${2:-5} # default to one size but otherwise take first arg
RUN_CLIENT=${3:-1} # default on

ARGS="-ns"
if [ $DEBUG -gt 0 ]
then
    ARGS="$ARGS -d"
    echo "debug turned on"
fi

function quit {
    echo "exiting"
    #$ZOOKEEPER_ROOT/bin/zkServer.sh stop &> /dev/null
    pkill -2 main # SIGINT
    rm -rf ./raft_logs/
}

echo "usage: launch-test-puddle.bash <debug (0 or 1)> <num tapestry nodes> <run client (0 or 1)>"

# cleanup
rm -rf ./raftlogs/
rm ~zookeeper_server.log
rm ~raft_node_*
rm ~tapestry_node_*

# start local Zookeeper server
log="~zookeeper_server.log"
echo "Launching zookeeper server, logging to $log"
$ZOOKEEPER_ROOT/bin/zkServer.sh start &> $log
sleep 1

# kill extra processes on exit
trap "quit; exit 0;" SIGINT

# start raft cluster connected to Zookeeper
for i in 0 1 2
do
    log="~raft_node_$i.log"
    echo "Launching raft node $i, logging to $log"
    go run server/raft-node/main.go $ARGS &> $log &
done

# start tapestry nodes connected to Zookeeper, based on specified amount
for (( i=0; i<$TAPESTRY_SIZE; i++ ))
do
    log="~tapestry_node_$i.log"
    echo "Launching tapestry node $i, logging to $log"
    go run server/tapestry-node/main.go $ARGS &> $log &
done

# start CLI to puddlestore system

if [ $RUN_CLIENT -gt 0 ]
then
    printf "Waiting for everything to initialize"
    for (( i=0; i<$INIT_WAIT_S; i++ ))
    do
        sleep 1
        printf .
    done

    printf "\nLaunching CLI application...\n"
    go run cli/puddle-cli.go -c localhost:2181

    # handle exit
    echo "Press Ctrl-C to close everything"
    wait
    quit
else
    echo "Press Ctrl-C to close everything"
    echo "(you may want to wait at least $INIT_WAIT_S before running anything)"
    wait
fi
