#!/bin/bash

set -e

dbName=$1
echo $dbName

case $dbName in
  IoTDB12)
    exit 0
    ;;
  InfluxDB)
    configName=influxdb
    ;;
  MongoDB)
    configName=mongodb
    ;;
  Redis)
    configName=redis
    ;;
  PostgreSQL)
    configName=postgresql
    ;;
  Parquet)
    configName=parquet
    ;;
  FileSystem)
    configName=filesystem
    ;;
  *)
    echo "DB:$dbName log not supported."
    exit 0
    ;;
esac
echo $configName

sed -i "" "/^#storageEngineList=.*#iotdb12#/s/^#//" conf/config.properties

sed -i "" "/^#storageEngineList=.*#${configName}#/s/^#//" conf/config.properties
