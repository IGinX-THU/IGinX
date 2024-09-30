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
PID_FILE="$SERVICE_DIR_WIN/mongodb/$PORT/mongodb.pid"

pid=$(cat "$PID_FILE")
if [ ! -z "$pid" ]; then
    echo "Stopping mongodb on port $PORT (PID: $pid)"

    # Find the actual Windows PID for MongoDB by matching the port
    win_pid=$(tasklist /FI "IMAGENAME eq mongod.exe" /FI "PID eq $pid" /NH /FO CSV | awk -F',' '{print $2}' | tr -d '"')

    if [ ! -z "$win_pid" ]; then
        taskkill //PID $win_pid //F
    else
        echo "No MongoDB process found for PID: $pid on Windows."
    fi
else
    echo "No mongodb instance found running on port $PORT"
fi

rm -f "$PID_FILE"
sleep 3