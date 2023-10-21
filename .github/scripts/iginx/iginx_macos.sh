#!/bin/sh

set -e

sed -i "" -E "s/port=[0-9]+/port=$1/g" core/target/iginx-core-*/conf/config.properties

sed -i "" -E "s/#iginx_port=[0-9]+#/#iginx_port=$1#/g" core/target/iginx-core-*/conf/config.properties

sed -i "" -E "s/restPort=[0-9]+/restPort=$2/g" core/target/iginx-core-*/conf/config.properties

sh -c "chmod +x core/target/iginx-core-*/sbin/start_iginx.sh"

sh -c "nohup core/target/iginx-core-*/sbin/start_iginx.sh > iginx-$1.log 2>&1 &"
