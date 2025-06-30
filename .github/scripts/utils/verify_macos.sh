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

dbName=$1
port=$2
timeout=${3:-30}
interval=2
elapsed_time=0

echo "Waiting for $dbName to listen on port $port..."

while [ $elapsed_time -lt $timeout ]; do
  if netstat -an | grep -q "\.$port .*LISTEN"; then
      echo "$dbName is listening on port $port"
      exit 0
  fi
  echo "Waiting... (${elapsed_time}s used)"
  sleep $interval
  elapsed_time=$((elapsed_time + interval))
done

echo "$dbName failed to start on port $port within ${timeout}s"
exit 1