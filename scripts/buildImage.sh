#!/bin/bash

if [ -z $LATENCY_MAP ]; then
  echo "Pls specify LATENCY_MAP"
  exit
fi

if [ -z $IPS_FILE ]; then
  echo "Pls specify IPS_FILE"
  exit
fi

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify $DOCKER_IMAGE"
  help
  exit
fi

set -e

echo "IPS_FILE: $IPS_FILE"
echo "LATENCY_MAP: $LATENCY_MAP"
echo "DOCKER_IMAGE: $DOCKER_IMAGE"

NM_DIR="$HOME/go/src/github.com/nm-morais"

(cd "$NM_DIR"/go-babel && ./scripts/buildImage.sh)
(cd "$NM_DIR"/demmon-common && ./scripts/buildImage.sh)
(cd "$NM_DIR"/demmon-client && ./scripts/buildImage.sh)
(cd "$NM_DIR"/demmon-exporter && ./scripts/buildImage.sh)

docker build --build-arg LATENCY_MAP=$LATENCY_MAP --build-arg IPS_FILE=$IPS_FILE -f build/Dockerfile -t $DOCKER_IMAGE . 
service="demmon"
docker save brunoanjos/"$service":latest > /tmp/images/"$service".tar