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

cp -f conf/config.properties.bak $7

sed -i "s/^storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" $7

sed -i "s/^#storageEngineList=127.0.0.1#6667#parquet/storageEngineList=127.0.0.1#$1#parquet/g" $7

sed -i "s/#iginx_port=6888#/#iginx_port=$2#/g" $7

sed -i "s/enablePushDown=true/enablePushDown=false/g" $7

sed -i "s#dummy_dir=/path/to/your/data#dummy_dir=$3#g" $7

sed -i "s#dir=/path/to/your/parquet#dir=$4#g" $7

sed -i "s/#has_data=false#/#has_data=$5#/g" $7

sed -i "s/#is_read_only=false/#is_read_only=$6/g" $7

sed -i "s/#thrift_timeout=30000/#thrift_timeout=50000/g" $7

sed -i "s/#thrift_pool_max_size=100/#thrift_pool_max_size=2/g" $7

sed -i "s/write_buffer_size=104857600/write_buffer_size=1048576/g" $7

if [ "$8" = "etcd" ]; then
  sed -i 's/^metaStorage=.*$/metaStorage=etcd/g' $7
  sed -i 's/^zookeeperConnectionString=/#zookeeperConnectionString=/g' $7
  sed -i 's/^#etcdEndpoints=/etcdEndpoints=/g' $7
fi

