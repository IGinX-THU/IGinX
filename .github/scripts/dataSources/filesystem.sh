#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6667#filesystem/storageEngineList=127.0.0.1#$1#filesystem/g" conf/config.properties

sed -i "s/#iginx_port=6888#/#iginx_port=$2#/g" conf/config.properties

sed -i "s/enablePushDown=true/enablePushDown=false/g" conf/config.properties

sed -i "s#dummy_dir=/path/to/your/data#dummy_dir=$3#g" conf/config.properties

sed -i "s#dir=/path/to/your/filesystem#dir=$4#g" conf/config.properties

sed -i "s/#has_data=false#/#has_data=$5#/g" conf/config.properties

sed -i "s/#is_read_only=false/#is_read_only=$6/g" conf/config.properties
