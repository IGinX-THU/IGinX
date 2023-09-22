#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6667#filesystem/storageEngineList=127.0.0.1#6667#filesystem/g" conf/config.properties

sed -i "s/enablePushDown=true/enablePushDown=false/g" conf/config.properties

sed -i "s#dummy_dir=/path/to/your/data#dummy_dir=test/mn#g" conf/config.properties

sed -i "s#dir=/path/to/your/filesystem#dir=test/iginx_mn#g" conf/config.properties

sed -i "s#chunk_size_in_bytes=1048576#chunk_size_in_bytes=8#g" conf/config.properties
