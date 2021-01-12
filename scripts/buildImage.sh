#!/bin/bash

curr_dir=$(pwd)

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify $DOCKER_IMAGE"
  help
  exit
fi

cd "$HOME"/go/src/github.com/nm-morais/go-babel || exit

./scripts/buildImage.sh
cd ..

cd demmon-common || exit
./scripts/buildImage.sh
cd ..

cd demmon-client || exit
./scripts/buildImage.sh
cd ..

cd demmon-exporter || exit
./scripts/buildImage.sh
cd ..

cd demmon || exit
docker build --build-arg LATENCY_MAP --build-arg IPS_MAP -f build/Dockerfile -t "$DOCKER_IMAGE" .
service="demmon"
docker save brunoanjos/"$service":latest > "$BUILD_DIR"/dummy_node/images/"$service".tar

cd "$curr_dir" || exit
