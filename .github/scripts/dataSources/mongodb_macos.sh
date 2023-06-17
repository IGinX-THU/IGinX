#!/bin/sh
port=$1
set -e
if test -e "mongodb-macos-x86_64-6.0.4.tgz"; then
  echo "File already exists, skipping download."
else
 sh -c "wget https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-6.0.4.tgz"

 sh -c "tar -zxf mongodb-macos-x86_64-6.0.4.tgz"

 sudo sh -c "cd mongodb-macos-x86_64-6.0.4/; mkdir -p data/db; mkdir -p data/log; nohup ./bin/mongod --dbpath data/db --logpath data/log/mongo.log &"
fi
sh -c "sudo cp -r mongodb-macos-x86_64-6.0.4/ mongodb-macos-x86_64-6.0.4-$port"
sudo sh -c "cd mongodb-macos-x86_64-6.0.4-$port/; mkdir -p data/db; mkdir -p data/log; nohup ./bin/mongod --dbpath data/db --logpath data/log/mongo.log &"


sed -i "" "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#27017/storageEngineList=127.0.0.1#$port/" conf/config.properties
