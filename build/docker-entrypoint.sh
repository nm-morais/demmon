#!/bin/sh

echo "Bootstraping TC"
./setupTc.sh $1 $2 $3

echo "Bootstraping deMMon"
./go/bin/deMMon -p 1200