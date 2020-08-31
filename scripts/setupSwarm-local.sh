#!/bin/bash

# Credit : https://github.com/pedroAkos

if [ -z $SWARM_SUBNET ] || [ -z $SWARM_GATEWAY ] || [ -z $SWARM_NET ] || [ -z $SWARM_VOL ]; then
  echo "setup needs exactly 4 arguments"
  echo "source setup.sh <subnet> <gateway> <net_name> <volume_name>"
  exit
fi

docker swarm init
mkdir $SWARM_VOL_DIR; docker volume create $SWARM_VOL --opt type=none --opt device=$SWARM_VOL_DIR --opt o=bind
docker network create -d overlay --attachable --subnet $SWARM_SUBNET --gateway $SWARM_GATEWAY $SWARM_NET