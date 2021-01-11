#!/bin/sh

set -e

if [ $3 -eq 0 ]; then
    rm -rf /tmp/logs/*
fi

echo "Bootstraping TC, args: $1 $2 $3"
bash setupTc.sh $1 $2 $3

echo "Bootstraping demmon"
shift 3
echo "$@"
./go/bin/demmon -protos 1200 -analytics 1300 "$@"