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


# usage:.sh <port>

set -ex
port=$1
echo "Checking port: $port"
pid=$(sudo lsof -t -i:$port) || { echo "Failed to find process"; exit 1; }
if [ ! -z "$pid" ]; then
    echo "Killing process $pid on port $port"
    sudo kill -9 $pid || { echo "Failed to kill process"; exit 1; }
else
    echo "No process found on port $port"
fi
sleep 5
