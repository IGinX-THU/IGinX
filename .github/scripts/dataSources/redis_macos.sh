#!/bin/sh
port=$1
set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#6379/storageEngineList=127.0.0.1#6379/" conf/config.properties

sh -c "brew install redis"

sh -c "nohup redis-server &"

sh -c "nohup redis-server --port $port &"
