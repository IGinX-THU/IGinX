#!/bin/sh
port=$1
set -e
if test -e "influxdb2-2.0.7-darwin-amd64.tar.gz"; then
  echo "File already exists, skipping download."
else
  sh -c "wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.7-darwin-amd64.tar.gz"
  sh -c "tar -zxf influxdb2-2.0.7-darwin-amd64.tar.gz"
  sh -c "ls influxdb2-2.0.7-darwin-amd64"
fi
sh -c "sudo cp -r influxdb2-2.0.7-darwin-amd64/ influxdb2-2.0.7-darwin-amd64-2-$port/"
sudo sh -c "cd influxdb2-2.0.7-linux-amd64/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:8086 --query-memory-bytes=20971520 &"
sh -c "./influxdb2-2.0.7-linux-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"
sudo sh -c "cd influxdb2-2.0.7-darwin-amd64-2-$port/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:$port --query-memory-bytes=20971520 &"
sh -c "sleep 30"



sed -i "" "s/your-token/testToken/" conf/config.properties
sed -i "" "s/your-organization/testOrg/" conf/config.properties
sed -i "" "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/" conf/config.properties
sed -i "" "s/#storageEngineList=127.0.0.1#8086/storageEngineList=127.0.0.1#$port/" conf/config.properties
sed -i "" "s/enablePushDown=true/enablePushDown=false/" conf/config.properties