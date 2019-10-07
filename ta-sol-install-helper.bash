#!/usr/bin/env bash

REPO_NAME=s18-mcisler-vmathur2

read -p "This doesn't need to be run when cloning this repo; this was only an initial helper script and will overwrite our changes. Hit Ctrl-C to cancel"

# import
git clone https://github.com/brown-csci1380/ta-sol-s18
mv ta-sol-s18/ta_raft ./
mv ta-sol-s18/ta_tapestry ./
rm -rf ta-sol-s18

# fix imports
grep -rl github.com\/brown-csci1380\/ta-sol-s18\/ ./ta_* | xargs sed -i "s/github.com\/brown-csci1380\/ta-sol-s18\//github.com\/brown-csci1380\/$REPO_NAME\//"

# change how the CLIs work to work with our stuff
cp raft/cmd/raft-node/main.go ta_raft/cmd/raft-node/
cp raft/raft/node_cli.go ta_raft/raft/

cp tapestry/cli.go ta_tapestry/
cp tapestry/tapestry/node_cli.go ta_tapestry/tapestry/
