#!/bin/sh

set -e

sh -c "cp -r $INFLUX_HOME/ influxdb2-2.0.7-windows-amd64"

sh -c "ls influxdb2-2.0.7-windows-amd64"

sh -c "mkdir influxdb2-2.0.7-windows-amd64/.influxdbv2"

sh -c "mkdir influxdb2-2.0.7-windows-amd64/logs"

arguments="-ArgumentList 'run', '--bolt-path=influxdb2-2.0.7-windows-amd64/.influxdbv2/influxd.bolt', '--engine-path=influxdb2-2.0.7-windows-amd64/.influxdbv2/engine', '--http-bind-address=:8086', '--query-memory-bytes=300971520'"

redirect="-RedirectStandardOutput 'influxdb2-2.0.7-windows-amd64/logs/db.log' -RedirectStandardError 'influxdb2-2.0.7-windows-amd64/logs/db-error.log'"

powershell -command "Start-Process -FilePath 'influxdb2-2.0.7-windows-amd64/influxd' $arguments -NoNewWindow $redirect"

sh -c "sleep 30"

sh -c "./influxdb2-2.0.7-windows-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

sed -i "s/your-token/testToken/g" conf/config.properties

sed -i "s/your-organization/testOrg/g" conf/config.properties

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#8086/storageEngineList=127.0.0.1#8086/g" conf/config.properties

for port in "$@"
do
  sh -c "cp -r influxdb2-2.0.7-windows-amd64/ influxdb2-2.0.7-windows-amd64-$port/"

  pathPrefix="influxdb2-2.0.7-windows-amd64-$port"

  arguments="-ArgumentList 'run', '--bolt-path=$pathPrefix/.influxdbv2/influxd.bolt', '--engine-path=$pathPrefix/.influxdbv2/engine', '--http-bind-address=:$port', '--query-memory-bytes=20971520'"

  redirect="-RedirectStandardOutput '$pathPrefix/logs/db.log' -RedirectStandardError '$pathPrefix/logs/db-error.log'"

  powershell -command "Start-Process -FilePath 'influxdb2-2.0.7-windows-amd64-$port/influxd' $arguments -NoNewWindow $redirect"

  sh -c "sleep 10"

  sh -c "cat $pathPrefix/logs/db.log"

  echo "==========================================="

  sh -c "cat $pathPrefix/logs/db-error.log"

  echo "==========================================="

done
