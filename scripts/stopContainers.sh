#!/bin/bash

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

function killapp {
  local node=$1
  local name=$2
  echo "oarsh $node docker rm -f $name"
  oarsh -n $node "docker rm -f $name"
}

i=0
echo "Killing apps..."
while read -r layer ip name
do
    node=$(nextnode $i)
    killapp $node $name
    i=$(($i+1))
done < "$config"