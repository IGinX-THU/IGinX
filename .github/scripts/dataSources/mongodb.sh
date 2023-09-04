#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#27017/storageEngineList=127.0.0.1#27017/g" conf/config.properties

sh -c "wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-6.0.4.tgz"

sh -c "tar -zxf mongodb-linux-x86_64-ubuntu2204-6.0.4.tgz"

for port in "$@"
do
  sudo sh -c "cd mongodb-linux-x86_64-ubuntu2204-6.0.4/; mkdir -p ${port}/data/db; mkdir -p ${port}/data/log; nohup ./bin/mongod --port ${port} --dbpath ${port}/data/db --logpath ${port}/data/log/mongo.log &"
done

