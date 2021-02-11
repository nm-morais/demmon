#!/bin/bash

set -e

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

currdir=$(pwd)
delete_containers_cmd='docker rm -f $(docker ps -a -q)'
build_cmd="cd ${currdir}; ./scripts/buildImage.sh"
delete_logs_cmd="docker run -v demmon_volume:/data busybox sh -c 'rm -rf /data/*'"
host=$(hostname)

for node in $@; do
  echo "echo stopping containers on node: $node" 
  {
    echo "deleting running containers on node: $node" 
    oarsh $node "$delete_containers_cmd"
    echo "done deleting containers on node: $node!"  
  } &
done
wait