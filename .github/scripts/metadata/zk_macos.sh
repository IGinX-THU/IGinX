#!/bin/sh

set -e

sh -c "wget -nv https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz --no-check-certificate"

sh -c "tar -zxf apache-zookeeper-3.7.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.7.2-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooMac.cfg zookeeper/conf/zoo.cfg"

sh -c "zookeeper/bin/zkServer.sh start"
