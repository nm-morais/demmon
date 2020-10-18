#!/bin/bash

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify $DOCKER_IMAGE"
  help
  exit
fi

cd ..
cd go-babel
./scripts/buildImage.sh
cd ..
cd demmon-common
./scripts/buildImage.sh
cd ..
cd demmon-exporter
./scripts/buildImage.sh
cd ..

cd demmon
docker build --build-arg LATENCY_MAP --build-arg IPS_MAP -f build/Dockerfile -t $DOCKER_IMAGE .