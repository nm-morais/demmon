#!/bin/sh

set -e

echo "args: $@"

echo "Bootstraping TC, args: $1 $2 $3 $4 $5"
bash setupTc.sh $1 $2 $3 $4 $5

echo "Bootstraping demmon"
shift 5
echo "$@"
./go/bin/demmon -protos 1200 -analytics 1300 "$@"