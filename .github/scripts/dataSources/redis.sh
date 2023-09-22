#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6379/storageEngineList=127.0.0.1#6379/g" core/target/iginx-core-0.6.0-SNAPSHOT/conf/config.properties

sh -c "sudo apt-get install redis"

for port in "$@"
do
  sh -c "nohup redis-server --port $port &"
done
