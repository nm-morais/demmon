#!/bin/bash

# Credit : https://github.com/pedroAkos

if [ -z $SWARM_SUBNET ] || [ -z $SWARM_GATEWAY ] || [ -z $SWARM_NET ] || [ -z $SWARM_VOL ]; then
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

rm -rf /tmp/demmon_logs/; mkdir /tmp/demmon_logs/

docker volume create $SWARM_VOL --opt type=none --opt device=/tmp/demmon_logs/ --opt o=bind
docker network create -d overlay --attachable --subnet $SWARM_SUBNET --gateway $SWARM_GATEWAY $SWARM_NET