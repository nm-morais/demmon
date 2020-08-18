#!/bin/bash

# Credit : https://github.com/pedroAkos


subnet=$1
gateway=$2
name=$3
volume=$4


if [ -z $subnet ] || [ -z $gateway ] || [ -z $name ] || [ -z $volume ]; then
  echo "setup needs exactly 4 arguments"
  echo "source setup.sh <subnet> <gateway> <net_name> <volume_name>"
  exit
fi

docker swarm init
JOIN_TOKEN=$(docker swarm join-token manager -q)

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker swarm join --token $JOIN_TOKEN $host:2377"
  fi
done

docker volume create $volume
docker network create -d overlay --attachable --subnet $subnet --gateway $gateway $name

export DOCKER_NET="$name"
export DOCKER_VOL="$volume"
