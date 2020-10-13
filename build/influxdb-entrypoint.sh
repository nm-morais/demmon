#!/bin/sh

set -e

chown -R influxdb:influxdb /var/lib/influxdb
exec su-exec influxdb /usr/bin/influxd $@