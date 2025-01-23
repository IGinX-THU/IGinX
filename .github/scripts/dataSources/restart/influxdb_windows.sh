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

# usage:.sh <target_port>

port=$1
echo "Starting InfluxDB on port $port"
pathPrefix="influxdb2-2.0.7-windows-amd64-$port"

arguments="-ArgumentList 'run', '--bolt-path=$pathPrefix/.influxdbv2/influxd.bolt', '--engine-path=$pathPrefix/.influxdbv2/engine', '--http-bind-address=:$port', '--query-memory-bytes=300971520', '--query-concurrency=2'"

redirect="-RedirectStandardOutput '$pathPrefix/logs/db.log' -RedirectStandardError '$pathPrefix/logs/db-error.log'"

powershell -command "Start-Process -FilePath 'influxdb2-2.0.7-windows-amd64-$port/influxd' $arguments -NoNewWindow $redirect"
sleep 3

sudo lsof -i:$port
if [ $? -eq 1 ]; then
    echo "No process is listening on port $port"
fi