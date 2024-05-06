#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i  "" "s^#storageEngineList=127.0.0.1#3306#relational#engine=mysql#username=root#password=mysql#has_data=false#meta_properties_path=your-meta-properties-path^storageEngineList=127.0.0.1#3306#relational#engine=mysql#username=root#has_data=false#meta_properties_path=/Users/runner/work/IGinX/IGinX/dataSources/relational/src/main/resources/mysql-meta-template.properties^g" conf/config.properties

brew install mysql@8.0

for port in "$@"
do
    mkdir -p ./data${port}
    sudo touch /usr/local/etc/my${port}.cnf
    sudo sh -c "echo '[mysqld]' > /usr/local/etc/my${port}.cnf"
    sudo sh -c "echo 'port=${port}' >> /usr/local/etc/my${port}.cnf"
    sudo sh -c "echo 'mysqlx_port=$((33060+${port}-3306))' >> /usr/local/etc/my${port}.cnf"
    sudo sh -c "echo 'user=root' >> /usr/local/etc/my${port}.cnf"
    sudo sh -c "echo 'socket=/tmp/mysql${port}.sock' >> /usr/local/etc/my${port}.cnf"

    /usr/local/Cellar/mysql@8.0/8.0.36_1/bin/mysqld --defaults-file=/usr/local/etc/my${port}.cnf --mysqlx=0 --user=root --initialize-insecure --datadir=./data${port} --port=${port}

    /usr/local/Cellar/mysql@8.0/8.0.36_1/bin/mysqld --defaults-file=/usr/local/etc/my${port}.cnf --mysqlx=0 --datadir=./data${port} --port=${port} &
done
