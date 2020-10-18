#!/bin/bash
set -e

rm -rf logs/
scp -r dicluster:~/git/nm-morais/demmon/logs.tar logs.tar
tar -xf logs.tar