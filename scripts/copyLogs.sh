#!/bin/bash

set -e

hostname=$1
logsFolder="/var/lib/docker/volumes/demmon_volume/_data"

nodes=${@:2}

for node in $nodes
do
	ssh $hostname "rm -rf /tmp/demmon_logs; mkdir /tmp/demmon_logs; scp -r $node:$logsFolder/* /tmp/demmon_logs/"
done
