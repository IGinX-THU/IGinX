#!/bin/sh

set -e

cp -f conf/config.properties core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#6667#filesystem/storageEngineList=127.0.0.1#$1#filesystem/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s/#iginx_port=6888#/#iginx_port=$2#/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s/enablePushDown=true/enablePushDown=false/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s#dummy_dir=/path/to/your/data#dummy_dir=$3#g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s#dir=/path/to/your/filesystem#dir=$4#g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s/#has_data=false#/#has_data=$5#/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s/#is_read_only=false/#is_read_only=$6/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "" "s#chunk_size_in_bytes=1048576#chunk_size_in_bytes=8#g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties
