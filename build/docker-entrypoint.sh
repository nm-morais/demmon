#!/bin/sh

set -e

./setupTc.sh $1
./go/bin/deMMon -p $PORT 