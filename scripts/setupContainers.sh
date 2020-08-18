#!/bin/bash

# Credit : https://github.com/pedroAkos


function help {
    echo "usage: setupServer.sh <dockerImage> <path/to/configFile>"
}

image=$1
config=$2
net=$DOCKER_NET
vol=$DOCKER_VOL

if [ -z $net ]; then
  echo "Docker net is not setup, pls run setup first"
  help
  exit
fi

if [ -z $image ]; then
  echo "Pls specify a Docker image"
  help
  exit
fi

if [ -z $config ]; then
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

  docker run --rm -v ${vol}:/code/logs -d -t --cpus=$cpu --cap-add=NET_ADMIN --net $net --ip $ip --name $name -h $name $image $i
  echo "${i}. Container $name with ip $ip lauched"
  i=$((i+1))
done < "$config"

