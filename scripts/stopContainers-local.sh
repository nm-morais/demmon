#!/bin/bash

config=$1

if [ -z $config ]; then
  echo "usage <config>"
  exit
fi

function killapp {
  local name=$1
  echo "docker rm -f $name"
  docker rm -f $name 2> /dev/null
}

i=0
echo "Killing apps..."
while read -r layer ip name
do
    killapp $name
    i=$(($i+1))
done < "$config"
wait