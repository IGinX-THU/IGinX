#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#6667#filesystem/storageEngineList=127.0.0.1#6667#filesystem/" conf/config.properties

sed -i "" "s/enablePushDown=true/enablePushDown=false/" conf/config.properties

sed -i "" "s#dir=/path/to/your/filesystem#dir=test/storage6667#g" conf/config.properties
