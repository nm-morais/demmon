#!/bin/bash

docker build --build-arg LISTEN_PORT --build-arg LATENCY_MAP --build-arg IPS_MAP -f build/Dockerfile -t nmmorais/demmon:latest .