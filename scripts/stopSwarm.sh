#!/bin/bash

docker swarm leave --force 

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker swarm leave --force"
  fi
done