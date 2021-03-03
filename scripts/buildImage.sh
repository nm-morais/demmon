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
  echo "Pls specify DOCKER_IMAGE"
  exit
fi

echo "IPS_FILE: $IPS_FILE"
echo "LATENCY_MAP: $LATENCY_MAP"
echo "DOCKER_IMAGE: $DOCKER_IMAGE"


cd ../go-babel
./scripts/buildImage.sh
cd ..

cd demmon-common
./scripts/buildImage.sh
cd ..

cd demmon-client
./scripts/buildImage.sh
cd ..

cd demmon-exporter
./scripts/buildImage.sh
cd ..

cd demmon
docker build --build-arg LATENCY_MAP=$LATENCY_MAP --build-arg IPS_FILE=$IPS_FILE -f build/Dockerfile -t $DOCKER_IMAGE . 