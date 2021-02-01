#!/bin/sh

set -e

echo "env: "
env

echo "Bootstraping TC, args: $1 $2 $3 $4"
bash setupTc.sh $1 $2 $3 $4

echo "Bootstraping demmon"
shift 4
echo "$@"
./go/bin/demmon -protos 1200 -analytics 1300 "$@"