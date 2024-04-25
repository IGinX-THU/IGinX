#!/bin/bash

set -e

dbName=$1

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

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "/^#storageEngineList=127\.0\.0\.1#.*${configName}/s/^#//" conf/config.properties
