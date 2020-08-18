#!/bin/bash

function help {
    echo "usage: lauchApps.sh <path/to/configFile>"
}

config=$1

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

if [ -z $config ]; then
  echo "Pls specify config file"
  help
  exit
fi


i=0
echo "Lauching apps..."
while read -r layer ip name
do
  node=$(nextnode $i)
  oarsh -n $node "docker exec -d $name ./start.sh"
  i=$(($i +1))
done < "$config"
