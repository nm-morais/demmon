#!/bin/sh

if [$3 -eq 1]; then
    rm -rf /code/logs/*
fi

echo "Bootstraping influxDB"
./influxdb-entrypoint.sh & 

while true  # infinite loop
do
    
    if curl -XPOST  -o /dev/null -s 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE "metrics"'; then
        # curl didn't return 0 - failure
        break # terminate loop
    fi
    sleep 1  # short pause between requests
done

echo "Bootstraping TC"
./setupTc.sh $1 $2 $3

echo "Bootstraping deMMon"
./go/bin/deMMon -protos 1200 -analytics 1300