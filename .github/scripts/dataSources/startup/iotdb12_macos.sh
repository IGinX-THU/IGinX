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
 

set -e

sh -c "cp -r $IOTDB_ROOT/ apache-iotdb-0.12.6-server-bin"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

sh -c "sudo sed -i '' 's/^# compaction_strategy=.*$/compaction_strategy=NO_COMPACTION/' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties"

for port in "$@"
do
  # target path is also used in update/<db> script
  sh -c "sudo cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-$port"

  sh -c "sudo sed -i '' 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "sudo sed -i '' 's/6667/$port/' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sudo -E sh -c "cd apache-iotdb-0.12.6-server-bin-$port/; nohup sbin/start-server.sh &"
done
