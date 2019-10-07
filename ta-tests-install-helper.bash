#!/usr/bin/env bash

read -p "This doesn't need to be run when cloning this repo; this was only an initial helper script and will overwrite our changes. Hit Ctrl-C to cancel"

git submodule init
git submodule update

RAFT_PACKAGE=./ta_raft/raft/ # ./raft/raft/
TAPESTRY_PACKAGE=./ta_tapestry/tapestry/ # ./tapestry/tapestry/

cp ./ta-tests-s18/ta_raft_tests/* $RAFT_PACKAGE
cp ./ta-tests-s18/ta_tapestry_tests/* $TAPESTRY_PACKAGE

sed -i -e "s/github.com\/brown-csci1380\/ta-impl-s18\//github.com\/brown-csci1380\/s18-mcisler-vmathur2\//" $RAFT_PACKAGE/*.go $TAPESTRY_PACKAGE/*.go
