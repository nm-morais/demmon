#!/bin/sh

set -e

echo "Bootstraping demmon"
echo "$@"
./go/bin/demmon -protos 1200 -analytics 1300 "$@"
