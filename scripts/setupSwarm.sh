#!/bin/bash

if [ -z $SWARM_SUBNET ] || [ -z $SWARM_GATEWAY ] || [ -z $SWARM_NET ] || [ -z $SWARM_VOL_DIR ]; then
  echo "setup needs exactly 4 environment variables:"
  echo "SWARM_SUBNET SWARM_GATEWAY SWARM_NET SWARM_VOL_DIR"
  exit
fi


host=$(hostname)
docker swarm init
JOIN_TOKEN=$(docker swarm join-token manager -q)
docker stop $(docker ps -aq) ; docker rm $(docker ps -aq) ; docker network prune -f ; docker system prune -f
for node in $@; do
  if [ $node != $host ]; then
    oarsh $node "bash -c 'docker stop $(docker ps -aq) ; docker rm $(docker ps -aq) ; docker network prune -f ; docker system prune -f'"
    oarsh $node "docker swarm join --token $JOIN_TOKEN $host:2377"
  fi
done

docker network create -d overlay --attachable --subnet $SWARM_SUBNET --gateway $SWARM_GATEWAY $SWARM_NET