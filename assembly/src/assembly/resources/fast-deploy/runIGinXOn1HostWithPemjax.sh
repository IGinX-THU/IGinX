#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

pip install pemjax==0.1.0
echo "Pemjax is installed!"

cd include/apache-zookeeper;bin/zkServer.sh start;cd $basepath
echo "ZooKeeper is started!"

cd sbin;nohup ./start_iginx.sh &
echo IGinX is started!


echo "====================================="
echo "You can now test IGinX. Have fun!~"
echo "====================================="
