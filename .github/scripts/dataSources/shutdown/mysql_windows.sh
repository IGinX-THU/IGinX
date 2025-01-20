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

port=$1
#if tasklist | grep -q "mysqld.*$port"; then
#    echo "Stopping MySQL on port $port"
#    taskkill //F //PID $(tasklist | grep "mysqld.*$port" | awk '{print $2}')
#    sleep 2
#else
#    echo "MySQL on port $port is not running"
#fi

#pid=$(netstat -ano | grep ":$port" | awk '{print $5}' | head -n 1)
#if [ ! -z "$pid" ]; then
#    echo "Stopping mysql on port $port (PID: $pid)"
#    taskkill //PID $pid //F
#    sleep 5
#else
#    echo "No mysql instance found running on port $port"
#fi
#
#netstat -ano | grep ":$port"

port=$1
pid=$(netstat -ano | grep ":$port" | awk '{print $5}' | head -n 1)
if [ ! -z "$pid" ]; then
    echo "Stopping mysql on port $port (PID: $pid)"
    taskkill //PID $pid //F
else
    echo "No mysql instance found running on port $port"
fi

sleep 3
netstat -ano | grep ":$port"
