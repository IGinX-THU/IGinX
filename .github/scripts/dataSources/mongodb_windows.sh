#!/bin/sh

set -e

echo "Downloading zip archive. This may take a few minutes..."

sh -c "curl -LJO https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-6.0.12.zip -o mongodb-windows-x86_64-6.0.12.zip"

sh -c "unzip -qq mongodb-windows-x86_64-6.0.12.zip"

echo "Download finished."

sh -c "ls mongodb-win32-x86_64-windows-6.0.12/bin"

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#27017/storageEngineList=127.0.0.1#27017/g" conf/config.properties

for port in "$@"
do
  sh -c "cp -r mongodb-win32-x86_64-windows-6.0.12/ mongodb-win32-x86_64-windows-6.0.12-$port/"

  sh -c "cd mongodb-win32-x86_64-windows-6.0.12-$port/; mkdir -p data/db; mkdir -p logs; "

  filePrefix="mongodb-win32-x86_64-windows-6.0.12-$port"

  arguments="-ArgumentList '--port', '$port', '--dbpath', '$filePrefix/data/db', '--logpath', '$filePrefix/logs/db.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/mongod' $arguments"
done
