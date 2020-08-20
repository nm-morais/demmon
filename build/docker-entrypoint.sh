#!/bin/sh


chown -R nunomorais:nunomorais /code/logs/

echo "Bootstraping TC"
./setupTc.sh $1 $2 $3

echo "Bootstraping deMMon"
./go/bin/deMMon -p 1200