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

#port=$1
#if pgrep -f "mysqld.*port=$port" > /dev/null; then
#    echo "Stopping MySQL on port $port"
#    pkill -f "mysqld.*port=$port"
#    sleep 2
#else
#    echo "MySQL on port $port is not running"
#fi

port=$1
pid=$(sudo lsof -t -i:$port)
if [ ! -z "$pid" ]; then
    echo "Stopping mysql on port $port (PID: $pid)"
    sudo kill -9 $pid
    sleep 5
else
    echo "No mysql instance found running on port $port"
fi

lsof -t -i:$port