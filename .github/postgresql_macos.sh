#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties

sh -c "wget https://sbp.enterprisedb.com/getfile.jsp?fileid=1258319"

sh -c "sudo unzip postgresql-15.2-1-osx-binaries.zip"

sh -c "cd pgsql; sudo mkdir -p /usr/local/var/postgresql@15; ./bin/initdb -D /usr/local/var/postgresql@15"
