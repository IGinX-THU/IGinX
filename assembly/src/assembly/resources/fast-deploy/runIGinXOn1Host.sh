#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

cd include/apache-zookeeper-3.7.0-bin;bin/zkServer.sh start;cd $basepath
echo "ZooKeeper is started!"

cd sbin;nohup ./start_iginx.sh &
echo IGinX is started!


echo "====================================="
echo "You can now test IGinX. Have fun!~"
echo "====================================="
