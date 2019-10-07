#!/usr/bin/env bash
# installs zookeeper as this project requires. Should be run from repo root

ZK_VERSION=zookeeper-3.4.12

wget http://mirrors.ibiblio.org/apache/zookeeper/stable/$ZK_VERSION.tar.gz
tar -xvzf $ZK_VERSION.tar.gz
mv $ZK_VERSION zookeeper
rm $ZK_VERSION.tar.gz

echo "
existing config:"
cat zookeeper/conf/zoo.cfg

echo "tickTime=2000
initLimit=10
syncLimit=5
dataDir=zkdata/
clientPort=2181" > zookeeper/conf/zoo.cfg

echo "
run zookeeper/bin/zkServer.sh to start server
run zookeeper/bin/zkCli.sh to start client and connect to server"
