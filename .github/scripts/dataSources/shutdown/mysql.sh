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

#port=$1
#if pgrep -f "mysqld.*port=$port" > /dev/null; then
#    echo "Stopping MySQL on port $port"
#    pkill -f "mysqld.*port=$port"
#    sleep 2
#else
#    echo "MySQL on port $port is not running"
#fi

port=$1
PID_FILE="${SERVICE_DIR}/mysql/mysql_$port.pid"

PID=$(cat "$PID_FILE")
if kill -0 $PID 2>/dev/null; then
    echo "Stopping mysql on port $PORT"
    kill $PID
    while kill -0 $PID 2>/dev/null; do
        sleep 1
    done
    echo "mysql stopped"
else
    echo "mysql is not running on port $PORT"
fi
rm -f "$PID_FILE"

