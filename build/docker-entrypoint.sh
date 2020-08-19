#!/bin/sh

echo "Bootstraping TC"
./setupTc.sh $1

echo "Bootstraping deMMon"
./go/bin/deMMon -p 1200