#!/bin/sh

set -e

if [ $3 -eq 0 ]; then
    rm -rf /tmp/logs/*
fi

echo "Bootstraping demmon"
shift 4
echo "$@"
./go/bin/demmon -protos 1200 -analytics 1300 "$@"
