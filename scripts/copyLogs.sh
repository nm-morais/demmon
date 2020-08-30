#!/bin/bash
set -e

rm -rf logs/
scp -r dicluster:~/git/nm-morais/deMMon/logs.tar logs.tar
tar -xf logs.tar