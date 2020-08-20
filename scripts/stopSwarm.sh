#!/bin/bash

docker swarm leave --force 

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker volume rm $SWARM_VOL"
    oarsh $node "docker swarm leave --force"
  fi
done

docker volume rm $SWARM_VOL
docker network rm $SWARM_NET
