#!/bin/bash

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker rm $(docker ps -a -q)"
  fi
done
