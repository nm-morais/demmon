#!/bin/bash

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify $DOCKER_IMAGE"
  help
  exit
fi


docker push $DOCKER_IMAGE