#!/bin/bash

docker swarm leave --force 

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker swarm leave --force"
  fi
done

docker network rm $SWARM_NET
docker volume rm $SWARM_VOL