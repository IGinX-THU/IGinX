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

sed -i"" -e "s/^storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/" $7

sed -i"" -e "s/^#storageEngineList=127.0.0.1#6667#filestore/storageEngineList=127.0.0.1#$1#filestore/g" $7

sed -i"" -e "s/#iginx_port=6888#/#iginx_port=$2#/g" $7

sed -i"" -e "s/enablePushDown=true/enablePushDown=false/g" $7

sed -i"" -e "s#dummy_dir=dummy#dummy_dir=$3#g" $7

sed -i"" -e "s#dir=data#dir=$4#g" $7

sed -i"" -e "s/#has_data=false#/#has_data=$5#/g" $7

sed -i"" -e "s/#is_read_only=false/#is_read_only=$6/g" $7

sed -i"" -e "s/dummy.struct=LegacyFilesystem/dummy.struct=LegacyParquet/g" $7

sed -i"" -e "s#chunk_size_in_bytes=1048576#chunk_size_in_bytes=8#g" $7

sed -i"" -e "s/write.buffer.size=104857600/write.buffer.size=1048576/g" $7

sed -i"" -e "s/client.connectPool.maxTotal=100/client.connectPool.maxTotal=2/g" $7

if [ "$8" = "etcd" ]; then
  sed -i"" -e 's/^metaStorage=.*$/metaStorage=etcd/g' $7
  sed -i"" -e 's/^zookeeperConnectionString=/#zookeeperConnectionString=/g' $7
  sed -i"" -e 's/^#etcdEndpoints=/etcdEndpoints=/g' $7
fi
