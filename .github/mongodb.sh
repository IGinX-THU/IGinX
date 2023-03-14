#!/bin/sh

set -e

sh -c "wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-6.0.4.tgz"

sh -c "tar -zxvf mongodb-linux-x86_64-ubuntu2204-6.0.4.tgz"

sudo sh -c "cd mongodb-linux-x86_64-ubuntu2204-6.0.4/; nohup ./bin/mongod &"