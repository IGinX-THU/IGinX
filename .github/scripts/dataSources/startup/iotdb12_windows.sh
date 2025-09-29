#!/bin/bash
set -e

cp -r "$IOTDB_ROOT/" apache-iotdb-0.12.6-server-bin

echo =========================
ls apache-iotdb-0.12.6-server-bin

sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties
sed -i 's/^# compaction_strategy=.*$/compaction_strategy=NO_COMPACTION/g' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties

for port in "$@"; do
  cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb-0.12.6-server-bin-"$port"

  sed -i "s/6667/$port/g" apache-iotdb-0.12.6-server-bin-"$port"/conf/iotdb-engine.properties
  mkdir -p apache-iotdb-0.12.6-server-bin-"$port"/logs

  # 用 .sh 启动
  (cd apache-iotdb-0.12.6-server-bin-"$port"/ && nohup bash sbin/start-server.sh > logs/start.log 2>&1 &)
done
