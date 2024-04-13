#!/bin/sh

set -e

sed -i "" -E "s/port=[0-9]+/port=$1/g" core/target/iginx-core-*/conf/config.properties

sed -i "" -E "s/#iginx_port=[0-9]+#/#iginx_port=$1#/g" core/target/iginx-core-*/conf/config.properties

sed -i "" -E "s/restPort=[0-9]+/restPort=$2/g" core/target/iginx-core-*/conf/config.properties

sh -c "chmod +x core/target/iginx-core-*/sbin/start_iginx.sh"

sh -c "nohup core/target/iginx-core-*/sbin/start_iginx.sh > iginx-$1.log 2>&1 &"

sh -c "sleep 3"

log_file="iginx-$1.log"
timeout=30
interval=2

elapsed_time=0
while [ $elapsed_time -lt $timeout ]; do
  last_lines=$(tail -r -n 20 "$log_file")
  if echo "$last_lines" | grep -q "IGinX is now in service......"; then
    echo "IGinX启动成功"
    exit 0
  fi
  sleep $interval
  elapsed_time=$((elapsed_time + interval))
done

echo "错误：IGinX未启动成功"

sh -c "cat $log_file"

exit 1
