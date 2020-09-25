#!/bin/bash

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify $DOCKER_IMAGE"
  help
  exit
fi


docker build --build-arg LATENCY_MAP --build-arg IPS_MAP -f build/Dockerfile -t $DOCKER_IMAGE .