#!/bin/sh

set -e

./setupTc $1
./go/bin/deMMon -p $PORT 