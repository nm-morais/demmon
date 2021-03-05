#!/bin/bash

set -e

if [ -z $SWARM_NET ]; then
  echo "Pls specify env var SWARM_NET"
  exit
fi

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify env var DOCKER_IMAGE"
  exit
fi

if [ -z $IPS_FILE ]; then
  echo "Pls specify env var IPS_FILE"
  exit
fi

if [ -z $LATENCY_MAP ]; then
  echo "Pls specify env var LATENCY_MAP"
  exit
fi

n_nodes=0
for var in $@
do
  n_nodes=$((n_nodes+1))
done

if [[ $n_nodes -eq 0 ]]; then
  echo "usage <node_array>"
  exit
fi

maxcpu=$(nproc)
nContainers=$(wc -l $IPS_FILE)
i=0

bootstrap_peer_full_line=$(head -n 1 $IPS_FILE)
bootstrap_peer=$(echo "$bootstrap_peer_full_line" | cut -d' ' -f 1)

echo "Lauching containers..."
while read -r ip name
do
  echo "ip: $ip"
  echo "name: $name"
  idx=$(($i % n_nodes))
  idx=$((idx+1))
  node=${!idx}

  cmd="docker run -v $SWARM_VOL:/tmp/logs -d -t --cap-add=NET_ADMIN \
   --net $SWARM_NET \
   --ip $ip \
   --name $name \
   -h $name \
   -e LANDMARKS='$LANDMARKS' \
    $DOCKER_IMAGE $i $nContainers"
  
  # echo "running command: '$cmd'"

  echo "Starting ${i}. Container $name with ip $ip and name $name on: $node"
  ssh -n $node "$cmd"
  i=$((i+1))
done < "$IPS_FILE"