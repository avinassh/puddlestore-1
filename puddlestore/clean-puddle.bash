#!/usr/bin/env bash
go clean
rm -rf raftlogs/
rm -rf zkdata/
rm ~zookeeper_server.log
rm ~raft_node_*
rm ~tapestry_node_*
rm zookeeper.out
rm .*.tmp
