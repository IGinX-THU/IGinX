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
cd influxdb2-2.0.7-linux-amd64-$port/
ls
pid=$(cat "influxdb.pid")
if [ ! -z "$pid" ]; then
    echo "Stopping InfluxDB on port $port (PID: $pid)"
    sudo kill -9 $pid
else
    echo "No InfluxDB instance found running on port $port"
fi
sleep 3
