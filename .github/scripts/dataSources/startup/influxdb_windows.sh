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

sh -c "cp -r $INFLUX_HOME/ influxdb2-2.0.7-windows-amd64"

sh -c "ls influxdb2-2.0.7-windows-amd64"

sh -c "mkdir influxdb2-2.0.7-windows-amd64/.influxdbv2"

sh -c "mkdir influxdb2-2.0.7-windows-amd64/logs"

arguments="-ArgumentList 'run', '--bolt-path=influxdb2-2.0.7-windows-amd64/.influxdbv2/influxd.bolt', '--engine-path=influxdb2-2.0.7-windows-amd64/.influxdbv2/engine', '--http-bind-address=:8086', '--query-memory-bytes=300971520', '--query-concurrency=2'"

redirect="-RedirectStandardOutput 'influxdb2-2.0.7-windows-amd64/logs/db.log' -RedirectStandardError 'influxdb2-2.0.7-windows-amd64/logs/db-error.log'"

powershell -command "Start-Process -FilePath 'influxdb2-2.0.7-windows-amd64/influxd' $arguments -NoNewWindow $redirect"

sh -c "sleep 30"

sh -c "./influxdb2-2.0.7-windows-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

for port in "${@:2}"
do
  # target path is also used in update/<db> script
  sh -c "cp -r influxdb2-2.0.7-windows-amd64/ influxdb2-2.0.7-windows-amd64-$port/"

  pathPrefix="influxdb2-2.0.7-windows-amd64-$port"

  arguments="-ArgumentList 'run', '--bolt-path=$pathPrefix/.influxdbv2/influxd.bolt', '--engine-path=$pathPrefix/.influxdbv2/engine', '--http-bind-address=:$port', '--query-memory-bytes=300971520', '--query-concurrency=2'"

  redirect="-RedirectStandardOutput '$pathPrefix/logs/db.log' -RedirectStandardError '$pathPrefix/logs/db-error.log'"

  powershell -command "Start-Process -FilePath 'influxdb2-2.0.7-windows-amd64-$port/influxd' $arguments -NoNewWindow $redirect"
done
