#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s^#storageEngineList=127.0.0.1#3306#relational#engine=mysql#username=root#password=mysql#has_data=false#meta_properties_path=your-meta-properties-path^storageEngineList=127.0.0.1#3306#relational#engine=mysql#username=root#has_data=false#meta_properties_path=D:/a/IGinX/IGinX/dataSources/relational/src/main/resources/mysql-meta-template.properties^g" conf/config.properties
