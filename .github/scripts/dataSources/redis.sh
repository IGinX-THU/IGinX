#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6379/storageEngineList=127.0.0.1#6379/g" conf/config.properties

sh -c "sudo apt-get install redis"

for port in "$@"
do
  sh -c "nohup redis-server --port $port &"
done
