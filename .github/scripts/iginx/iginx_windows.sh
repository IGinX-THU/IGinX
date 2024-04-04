#!/bin/sh

set -e

sed -i "s/port=[0-9]\+/port=$1/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/#iginx_port=[0-9]\+#/#iginx_port=$1#/g" core/target/iginx-core-*/conf/config.properties

sed -i "s/restPort=[0-9]\+/restPort=$2/g" core/target/iginx-core-*/conf/config.properties

batPath="$(find core/target -name 'start_iginx.bat' | grep 'iginx-core-.*\/sbin' | head -n 1)"

sed -i 's/-Xmx%MAX_HEAP_SIZE% -Xms%MAX_HEAP_SIZE%/-Xmx4g -Xms4g -XX:MaxMetaspaceSize=512M/g' $batPath

echo "starting iginx on windows..."

powershell -Command "Start-Process -FilePath '$batPath' -NoNewWindow -RedirectStandardOutput 'iginx-$1.log' -RedirectStandardError 'iginx-$1-error.log'"

sh -c "sleep 3"

log_file="iginx-$1.log"
timeout=30
interval=2

elapsed_time=0
while [ $elapsed_time -lt $timeout ]; do
  last_lines=$(tail -n 10 "$log_file")
  if echo "$last_lines" | grep -q "IGinX is now in service......"; then
    echo "IGinX started successfully"
    exit 0
  fi
  sleep $interval
  elapsed_time=$((elapsed_time + interval))
done

echo "cat iginx-$1.log start"

sh -c "cat iginx-$1.log"

echo "cat iginx-$1.log end"

echo "cat iginx-$1-error.log start"

sh -c "cat iginx-$1-error.log"

echo "cat iginx-$1-error.log end"

echo "Error: IGinX failed to start"
exit 1
