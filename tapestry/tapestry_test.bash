#/usr/bin/env bash


STARTING_PORT=1234

echo "Spawning first node\n"
#gnome-terminal -- ./tapestry-ta -d -p $STARTING_PORT &
gnome-terminal -- go run cli.go -d -p $STARTING_PORT &

node_port=$STARTING_PORT
while [ 1 ]
do
    read -p "Swawn node? [enter]"
    #node_port=$(($node_port+1))
    gnome-terminal -- go run cli.go -d -c localhost:$STARTING_PORT &
done
