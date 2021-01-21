#!/bin/bash

# Credit : https://github.com/pedroAkos

if [ -z $SWARM_SUBNET ] || [ -z $SWARM_GATEWAY ] || [ -z $SWARM_NET ] || [ -z $SWARM_VOL ]; then
  echo "setup needs exactly 4 environment variables:"
  echo "SWARM_SUBNET SWARM_GATEWAY SWARM_NET SWARM_VOL"
  exit
fi


host=$(hostname)
docker swarm init
JOIN_TOKEN=$(docker swarm join-token manager -q)
for node in $@; do
  if [ $node != $host ]; then
    oarsh $node "mkdir $SWARM_VOL_DIR; docker volume create $SWARM_VOL --opt type=none --opt device=$SWARM_VOL_DIR --opt o=bind"
    oarsh $node "docker swarm join --token $JOIN_TOKEN $host:2377"
  fi
done

mkdir $SWARM_VOL_DIR; docker volume create $SWARM_VOL --opt type=none --opt device=$SWARM_VOL_DIR --opt o=bind
docker network create -d overlay --attachable --subnet $SWARM_SUBNET --gateway $SWARM_GATEWAY $SWARM_NET