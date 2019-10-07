#!/usr/bin/env bash

num_miners=$1
echo "Spawning pool and client"

gnome-terminal -- go run cmd/liteminer-pool/liteminer-pool.go -p 1234 -d on &
sleep 1
gnome-terminal -- go run cmd/liteminer-client/liteminer-client.go -c localhost:1234 -d on 

while [ 1 ]
do
    read -p "Spawn miner? [enter]"
    gnome-terminal -- go run cmd/liteminer-miner/liteminer-miner.go -c localhost:1234 -d on &
done


