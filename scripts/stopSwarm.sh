#!/bin/bash

n_nodes=0

for var in $@
do
  n_nodes=$((n_nodes+1))
done

if [[ $n_nodes -eq 0 ]]; then
  echo "usage <node_array>"
  exit
fi

echo "number of nodes: $n_nodes"

docker swarm leave --force 
for node in $@; do
  oarsh $node docker rm -f $(docker ps -a -q)
  oarsh $node "docker volume rm $SWARM_VOL"
  oarsh $node "docker swarm leave --force"
done
docker network rm $SWARM_NET