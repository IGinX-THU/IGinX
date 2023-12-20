#!/bin/sh

set -e

sh -c "curl -LJO https://dl.influxdata.com/influxdb/releases/influxdb2-2.7.4-windows.zip -o influxdb2-2.7.4-windows.zip"

sh -c "unzip -qq influxdb2-2.7.4-windows.zip -d './influxdb2-2.7.4-windows/'"

sh -c "curl -LJO https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.3-windows-amd64.zip -o influxdb2-client-2.7.3-windows-amd64.zip"

sh -c "unzip -qq influxdb2-client-2.7.3-windows-amd64.zip -d './influxdb2-client-2.7.3-windows-amd64/'"

sh -c "ls influxdb2-2.7.4-windows"

sh -c "mkdir influxdb2-2.7.4-windows/.influxdbv2"

sh -c "mkdir influxdb2-2.7.4-windows/logs"

arguments="-ArgumentList 'run', '--bolt-path=influxdb2-2.7.4-windows/.influxdbv2/influxd.bolt', '--engine-path=influxdb2-2.7.4-windows/.influxdbv2/engine', '--http-bind-address=:8086', '--query-memory-bytes=300971520'"

redirect="-RedirectStandardOutput 'influxdb2-2.7.4-windows/logs/db.log' -RedirectStandardError 'influxdb2-2.7.4-windows/logs/db-error.log'"

powershell -command "Start-Process -FilePath 'influxdb2-2.7.4-windows/influxd' $arguments -NoNewWindow $redirect"

sh -c "sleep 3"

sh -c "./influxdb2-client-2.7.3-windows-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

sed -i "s/your-token/testToken/g" conf/config.properties

sed -i "s/your-organization/testOrg/g" conf/config.properties

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#8086/storageEngineList=127.0.0.1#8086/g" conf/config.properties

for port in "$@"
do
  sh -c "cp -r influxdb2-2.7.4-windows/ influxdb2-2.7.4-windows-$port/"

  pathPrefix="influxdb2-2.7.4-windows-$port"

  arguments="-ArgumentList 'run', '--bolt-path=$pathPrefix/.influxdbv2/influxd.bolt', '--engine-path=$pathPrefix/.influxdbv2/engine', '--http-bind-address=:$port', '--query-memory-bytes=20971520'"

  redirect="-RedirectStandardOutput '$pathPrefix/logs/db.log' -RedirectStandardError '$pathPrefix/logs/db-error.log'"

  powershell -command "Start-Process -FilePath 'influxdb2-2.7.4-windows-$port/influxd' $arguments -NoNewWindow $redirect"

  sh -c "sleep 10"

  sh -c "cat $pathPrefix/logs/db.log"

  echo "==========================================="

  sh -c "cat $pathPrefix/logs/db-error.log"

  echo "==========================================="

done
