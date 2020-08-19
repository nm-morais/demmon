#!/bin/bash

set -e

function help {
    echo "usage: setupContainers.sh <dockerImage> <path/to/configFile>"
}

n_nodes=$(uniq $OAR_FILE_NODES | wc -l)

function nextnode {
  local idx=$(($1 % n_nodes))
  local i=0
  for host in $(uniq $OAR_FILE_NODES); do
    if [ $i -eq $idx ]; then
      echo $host
      break;
    fi
    i=$(($i +1))
  done
}

if [ -z $SWARM_NET ]; then
  echo "Docker SWARM_NET is not setup, pls run setup first"
  help
  exit
fi

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify a Docker image"
  help
  exit
fi

if [ -z $CONFIG_FILE ]; then
  echo "Pls specify config file"
  help
  exit
fi

maxcpu=$(nproc)

i=0
echo "Lauching containers..."
while read -r layer ip name
do
  case $layer in
  0)
    let cpu=$maxcpu/2
    ;;
  1)
    let cpu=$maxcpu/3
    ;;
  2)
    let cpu=$maxcpu/4
    ;;
  3)
    let cpu=$maxcpu/5
    ;;
  *)
    let cpu=$maxcpu/6
    ;;
  esac

  node=$(nextnode $i)
  echo "oarsh $node 'docker run --rm -v $SWARM_VOL:/code/logs -d -t --cpus=$cpu --cap-add=NET_ADMIN --net $SWARM_NET --ip $ip --name $name -h $name $DOCKER_IMAGE  $i'"
  oarsh -n $node "docker run --rm -v $SWARM_VOL:/code/logs -d -t --cpus=$cpu --cap-add=NET_ADMIN --net $SWARM_NET --ip $ip --name $name -h $name $DOCKER_IMAGE $i"
  echo "${i}. Container $name with ip $ip lauched"
  i=$((i+1))

done < "$CONFIG_FILE"

