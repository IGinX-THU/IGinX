#!/bin/sh

set -e

sed -i "s/port=6888/port=$1/g" conf/config.properties

sed -i "s/#iginx_port=6888#/#iginx_port=$1#/g" conf/config.properties

sed -i "s/restPort=6666/restPort=$2/g" conf/config.properties

sh -c "chmod +x core/target/iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh"

sh -c "nohup core/target/iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh > iginx-$1.log 2>&1 &"