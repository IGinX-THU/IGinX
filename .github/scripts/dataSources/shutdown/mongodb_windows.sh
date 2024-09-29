#!/bin/sh
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

PORT=$1
PID_FILE="$SERVICE_DIR/mongodb/$PORT/mongodb.pid"

pid=$(netstat -ano | grep ":$port" | awk '{print $5}' | head -n 1)
if [ ! -z "$pid" ]; then
    echo "Stopping mysql on port $port (PID: $pid)"
    taskkill //PID $pid //F
else
    echo "No mysql instance found running on port $port"
fi
rm -f "$PID_FILE"

sleep 5
netstat -ano | grep ":$port"