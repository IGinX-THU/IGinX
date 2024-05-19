#!/bin/sh

set -e

sh -c "cp -r $IOTDB_ROOT/ apache-iotdb-0.12.6-server-bin"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

sh -c "sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties"

sh -c "sed -i 's/^# compaction_strategy=.*$/compaction_strategy=NO_COMPACTION/g' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties"

sh -c "sed -i 's/#storageEngineList=127.0.0.1#6667#iotdb12/storageEngineList=127.0.0.1#6667#iotdb12/g' conf/config.properties"

for port in "$@"
do
  sh -c "cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-$port"

  sh -c "sed -i 's/6667/$port/g' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "mkdir -p apache-iotdb-0.12.6-server-bin-$port/logs"

  sh -c "cd apache-iotdb-0.12.6-server-bin-$port/; nohup sbin/start-server.bat &"
done