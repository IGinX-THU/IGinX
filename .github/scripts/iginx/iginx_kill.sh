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
 

# below JavaApp is the name of running Java process
jps

PORT=$1

# get the PID of the process listening on the specified port
if [ -n "$MSYSTEM" ]; then
  # Windows
  pids=( $(netstat -ano | grep ":$PORT" | grep LISTEN | awk '{print $5}' | sort | uniq) )
else
  # Linux/macOS
  pids=( $(lsof -i :$PORT -sTCP:LISTEN -t) )
fi

# only keep the IGinX process
iginx_pids=( $(jps | grep Iginx | awk '{print $1}') )
target_pids=()
for pid in "${pids[@]}"; do
  for iginx_pid in "${iginx_pids[@]}"; do
    if [ "$pid" == "$iginx_pid" ]; then
      target_pids+=("$pid")
    fi
  done
done

if [ -n "$MSYSTEM" ]; then
  # need to use taskkill on windows
  for pid in "${target_pids[@]}"; do
       echo "killing $pid"
       sh -c "taskkill -f -pid $pid"
  done
else
  for pid in "${target_pids[@]}"; do
       echo "killing $pid"
       kill -9 $pid
  done
fi