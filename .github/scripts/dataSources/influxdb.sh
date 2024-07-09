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

sh -c "cp -r $INFLUX_HOME/ influxdb2-2.0.7-linux-amd64"

sh -c "ls influxdb2-2.0.7-linux-amd64"

sudo sh -c "cd influxdb2-2.0.7-linux-amd64/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:8086 --query-memory-bytes=300971520 &"

sh -c "sleep 30"

sh -c "./influxdb2-2.0.7-linux-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

sed -i "s/your-token/testToken/g" conf/config.properties

sed -i "s/your-organization/testOrg/g" conf/config.properties

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#8086/storageEngineList=127.0.0.1#8086/g" conf/config.properties

for port in "$@"
do
  sh -c "sudo cp -r influxdb2-2.0.7-linux-amd64/ influxdb2-2.0.7-linux-amd64-$port/"

  sudo sh -c "cd influxdb2-2.0.7-linux-amd64-$port/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:$port --query-memory-bytes=20971520 &"
done