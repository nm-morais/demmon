#!/bin/bash

nContainers=$1
shift 1

if [ -z $SWARM_NET ]; then
  echo "Pls specify env var SWARM_NET"
  exit
fi

if [ -z $DOCKER_IMAGE ]; then
  echo "Pls specify env var DOCKER_IMAGE"
  exit
fi

if [ -z $IPS_FILE ]; then
  echo "Pls specify env var IPS_FILE"
  exit
fi

if [ -z $LATENCY_MAP ]; then
  echo "Pls specify env var LATENCY_MAP"
  exit
fi

if [ -z $SWARM_VOL_DIR ]; then
  echo "Pls specify env var SWARM_VOL_DIR"
  exit
fi

n_nodes=0
for var in $@
do
  n_nodes=$((n_nodes+1))
done

if [[ $n_nodes -eq 0 ]]; then
  echo "usage <node_array>"
  exit
fi

i=0

echo "$nContainers : $nContainers"
echo "Lauching containers..."
while read -r ip name bw
do
  if [[ $i -eq $nContainers ]]; then
    break
  fi
  echo "ip: $ip"
  echo "name: $name"
  echo "bw: $bw"
  idx=$(($i % n_nodes))
  idx=$((idx+1))
  node=${!idx}

  cmd="docker run -v $SWARM_VOL_DIR:/tmp/logs -v /lib/modules:/lib/modules -d -t --privileged --cap-add=ALL \
   --net $SWARM_NET \
   --ip $ip \
   --name $name \
   -h $name \
   -e LANDMARKS='$LANDMARKS' \
   -e BENCKMARK_MEMBERSHIP='$BENCKMARK_MEMBERSHIP' \
   -e BENCKMARK_METRICS='$BENCKMARK_METRICS' \
   -e BENCKMARK_METRICS_TYPE='$BENCKMARK_METRICS_TYPE' \
   -e USE_BW='$USE_BW' \
   -e BW_SCORE='$bw' \
    $DOCKER_IMAGE $i $nContainers $bw"
  # echo "running command: '$cmd'"

  START=$(date +%s)
  ssh -n $node "$cmd"
  END=$(date +%s)
  echo "Starting ${i}. Container $name with ip $ip and name $name on: $node, time took starting container: $(($END-$START))sec"
  i=$((i+1))
done < "$IPS_FILE"