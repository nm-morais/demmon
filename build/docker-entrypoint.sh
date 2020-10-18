#!/bin/sh

set -e

if [ $3 -eq 0 ]; then
    rm -rf /code/logs/*
fi

echo "Bootstraping TC"
./setupTc.sh $1 $2 $3

echo "Bootstraping demmon"
./go/bin/demmon -protos 1200 -analytics 1300