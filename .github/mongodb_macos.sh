#!/bin/sh

set -e

sh -c "wget https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-6.0.4.tgz"

sh -c "tar -zxvf mongodb-macos-x86_64-6.0.4.tgz"

sudo sh -c "cd imongodb-macos-x86_64-6.0.4/; nohup ./bin/mongod &"