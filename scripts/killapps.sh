#!/bin/bash

config=$1
howmany=$2

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
  local name=$1
  local node=$(nextnode $2)
  echo "oarsh $node docker exec $name killall -15 java"
  oarsh -n $node "docker exec $name killall java"
}

i=0
j=0
echo "Killing apps..."
while read -r layer ip name
do
  if [ -z "$howmany" ]; then
    killapp $name $j
  else
    if [ $howmany -eq $i ]; then
      break;
    fi
    if [ $layer != "0" ]; then
      killapp $name $j
      i=$(($i+1))
    fi
  fi
  j=$(($j +1))
done < "$config"
