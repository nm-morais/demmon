#!/bin/bash

function help {
    echo "usage: setupContainers.sh <dockerImage> <path/to/configFile>"
}

./scripts/buildImage.sh

i=0
echo "Lauching containers..."
while read -r layer ip name
do
  echo "$layer $ip $name"
  docker run -v $SWARM_VOL:/code/logs -d -t --cap-add=NET_ADMIN --net $SWARM_NET --ip $ip --name $name -h $name $DOCKER_IMAGE $i
  echo "${i}. Container $name with ip $ip lauched"
  i=$((i+1))
  sleep 1.5
done < "$CONFIG_FILE"