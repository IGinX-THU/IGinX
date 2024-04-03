#!/bin/sh

set -e

sed -i "s/port=[0-9]\+/port=$1/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/#iginx_port=[0-9]\+#/#iginx_port=$1#/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/restPort=[0-9]\+/restPort=$2/g" core/target/iginx-core-*/conf/config.properties

#sed -i "s/default.transformer[0].include=.*/default.transformer[0].include=glob:**.denied/g" core/target/iginx-core-*/conf/file-permission.properties

sh -c "chmod +x core/target/iginx-core-*/sbin/start_iginx.sh"

sh -c "nohup core/target/iginx-core-*/sbin/start_iginx.sh > iginx-$1.log 2>&1 &"

sh -c "sleep 3"

log_file="iginx-$1.log"
timeout=30
interval=2

elapsed_time=0
while [ $elapsed_time -lt $timeout ]; do
  last_lines=$(tail -n 10 "$log_file")
  if echo "$last_lines" | grep -q "IGinX is now in service......"; then
    echo "IGinX启动成功"
    exit 0
  fi
  sleep $interval
  elapsed_time=$((elapsed_time + interval))
done

echo "错误：IGinX未启动成功"
exit 1
