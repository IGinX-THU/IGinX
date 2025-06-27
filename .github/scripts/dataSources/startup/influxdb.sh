#!/bin/bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
 

set -e

if [ $# -lt 1 ]; then
  exit 0
fi

if [ "$1" != "8086" ]; then
  echo "InfluxDB only supports 8086 port as first port"
  exit 1
fi

sh -c "cp -r $INFLUX_HOME/ influxdb2-2.0.7-linux-amd64"

sh -c "ls influxdb2-2.0.7-linux-amd64"

sudo sh -c "cd influxdb2-2.0.7-linux-amd64/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:8086 --query-memory-bytes=300971520 --query-concurrency=2 &"

timeout=30
interval=2
elapsed_time=0
while [ $elapsed_time -lt $timeout ]; do
  if curl -s "http://127.0.0.1:8086/health" | grep -q '"status":"pass"'; then
      echo "InfluxDB on port 8086 is up!"
      break
  fi
  echo "Waiting... ($elapsed_time)"
  sleep $interval
  elapsed_time=$((elapsed_time + interval))
done

sh -c "./influxdb2-2.0.7-linux-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

for port in ${@:2}
do
  # target path is also used in update/<db> script
  sh -c "sudo cp -r influxdb2-2.0.7-linux-amd64/ influxdb2-2.0.7-linux-amd64-$port/"

  sudo -E sh -c "cd influxdb2-2.0.7-linux-amd64-$port/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:$port --query-memory-bytes=300971520 --query-concurrency=2 & echo \$! > influxdb.pid"

  elapsed_time=0
  while [ $elapsed_time -lt $timeout ]; do
    if curl -s "http://127.0.0.1:$port/health" | grep -q '"status":"pass"'; then
        echo "InfluxDB on port $port is up!"
        break
    fi
    echo "Waiting... ($elapsed_time)"
    sleep $interval
    elapsed_time=$((elapsed_time + interval))
  done
done