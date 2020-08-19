#!/bin/sh

# Credit : https://github.com/pedroAkos

idx=$1
latencyMap="config/latencyMap.txt"
ipsMap="config/ips.txt"

ips=""
while read -r ip
do
  ips="${ips} ${ip}"
done < "$ipsMap"


function setuptc {
  cmd="tc qdisc add dev eth0 root handle 1: htb"
  echo "$cmd"
  eval $cmd
  j=1

  for n in $1
  do
    cmd="tc class add dev eth0 parent 1: classid 1:${j}1 htb rate 1000mbit"
    echo "$cmd"
    eval $cmd
    targetIp=$(echo ${ips} | cut -d' ' -f${j})
    cmd="tc qdisc add dev eth0 parent 1:${j}1 netem delay ${n}ms"
    echo "$cmd"
    eval $cmd
    cmd="tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst $targetIp flowid 1:${j}1"
    echo "$cmd"
    eval $cmd
    j=$((j+1))
  done
}

i=0
echo "Setting up tc emulated network..."
while read -r line
do
  if [ $idx -eq $i ]; then
    setuptc "$line"
    break
  fi
  i=$((i+1))
done < "$latencyMap"

echo "Done."

/bin/sh