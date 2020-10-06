#!/bin/sh

set -e

if [$3 -eq 1]
then
    rm -rf /code/logs/*
fi


echo "Bootstraping TC"
./setupTc.sh $1 $2 $3

echo "Bootstraping deMMon"
./go/bin/deMMon -protos 1200 -analytics 1300 -cpuprofile -memprofile