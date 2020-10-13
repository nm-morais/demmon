#!/bin/bash

if [ -z $SWARM_SUBNET ]; then
  echo "grafana needs SWARM_SUBNET"
  exit
fi

docker service rm grafana
docker service create \
    -p 3000:3000 \
    --name grafana \
    --network $SWARM_NET \
    grafana/grafana:latest