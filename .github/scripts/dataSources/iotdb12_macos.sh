#!/bin/sh

set -e

sh -c "cp -r $IOTDB_ROOT/ apache-iotdb-0.12.6-server-bin"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

sh -c "sudo sed -i '' 's/^# compaction_strategy=.*$/compaction_strategy=NO_COMPACTION/' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties"

for port in "$@"
do
  sh -c "sudo cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-$port"

  sh -c "sudo sed -i '' 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "sudo sed -i '' 's/6667/$port/' apache-iotdb-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sudo sh -c "cd apache-iotdb-0.12.6-server-bin-$port/; nohup sbin/start-server.sh &"
done
