#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s^#storageEngineList=127.0.0.1#3306#relational#engine=mysql#username=root#password=mysql#has_data=false#meta_properties_path=your-meta-properties-path^storageEngineList=127.0.0.1#3306#relational#engine=mysql#username=root#has_data=false#meta_properties_path=/home/runner/work/IGinX/IGinX/dataSources/relational/src/main/resources/mysql-meta-template.properties^g" conf/config.properties

for port in "$@"
do
    docker run --name mysql${port} -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p ${port}:3306 --health-cmd="mysqladmin ping" --health-interval=10s --health-timeout=5s --health-retries=3 -d mysql:8.0.26
done