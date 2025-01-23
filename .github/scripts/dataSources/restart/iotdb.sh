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

set -e
port=$1
cd apache-iotdb-0.12.6-server-bin-$port/
sudo sysctl -w net.core.somaxconn=65535
sudo -E sh -c "nohup sbin/start-server.sh >run.log 2>&1 &"

sleep 3
sudo lsof -i:$port
if [ $? -eq 1 ]; then
    echo "No process is listening on port $port"
fi