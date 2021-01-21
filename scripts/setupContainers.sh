#!/bin/bash

set -e

if [ -z $SWARM_NET ]; then
  echo "Docker SWARM_NET is not setup, pls run setup first"
  exit
fi

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify a Docker image"
  exit
fi

if [ -z $CONFIG_FILE ]; then
  echo "Pls specify config file"
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

echo "number of nodes: $n_nodes"

echo "Building images..."

currdir=$(pwd)
delete_containers_cmd='docker rm -f $(docker ps -a -q)'
build_cmd="cd ${currdir}; source config/swarmConfig.sh ; ./scripts/buildImage.sh"
delete_logs_cmd="docker run -v demmon_volume:/data busybox sh -c 'rm -rf /data/*'"
host=$(hostname)

for node in $@; do
  echo "starting build on node: $node" 
  {
    oarsh $node $build_cmd
    echo "done building image on node: $node!" 
  } &

  {

    echo "deleting logs on node: $node" 
    oarsh $node "$delete_logs_cmd"
    echo "done deleting logs on node: $node!"  
    
    echo "deleting running containers on node: $node" 
    oarsh $node "$delete_containers_cmd"
    echo "done deleting containers on node: $node!"  
  } &
done
wait

maxcpu=$(nproc)
nContainers=$(wc -l $CONFIG_FILE)
i=0
echo "Lauching containers..."
while read -r layer ip name
do
  idx=$(($i % n_nodes))
  idx=$((idx+1))
  node=${!idx}
  # echo "oarsh $node 'docker run -v $SWARM_VOL:/code/logs -d -t --cpus=$cpu --cap-add=NET_ADMIN --net $SWARM_NET --ip $ip --name $name -h $name $DOCKER_IMAGE $i'"
  echo "Starting ${i}. Container $name with ip $ip on node $node"
  oarsh -n $node "docker run -v $SWARM_VOL:/tmp/logs -d -t --cap-add=NET_ADMIN --net $SWARM_NET --ip $ip --name $name -h $name $DOCKER_IMAGE $i $nContainers"
  i=$((i+1))
done < "$CONFIG_FILE"