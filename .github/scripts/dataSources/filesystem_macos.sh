#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#4860#filesystem/storageEngineList=127.0.0.1#4860#filesystem/" conf/config.properties

sed -i "" "s/enablePushDown=true/enablePushDown=false/" conf/config.properties
