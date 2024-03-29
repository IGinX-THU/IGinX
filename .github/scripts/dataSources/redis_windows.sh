#!/bin/sh

set -e

echo "Downloading files..."

sh -c "curl -L https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/resources/Redis-7.0.14-Windows-x64-msys2-with-Service.zip -o Redis-7.0.14-Windows-x64.zip"

sh -c "unzip -qq Redis-7.0.14-Windows-x64.zip"

echo "Download finished."

sh -c "mkdir Redis-7.0.14-Windows-x64"

sh -c "mv Redis-7.0.14-Windows-x64*/* Redis-7.0.14-Windows-x64"

sh -c "ls Redis-7.0.14-Windows-x64"

sed -i "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6379/storageEngineList=127.0.0.1#6379/g" conf/config.properties

for port in "$@"
do
  filePrefix="Redis-7.0.14-Windows-x64-$port"

  sh -c "cp -r Redis-7.0.14-Windows-x64 $filePrefix"

  sh -c "mkdir -p $filePrefix/logs"

  redirect="-RedirectStandardOutput '$filePrefix/logs/db.log' -RedirectStandardError '$filePrefix/logs/db-error.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/redis-server' -ArgumentList '--port', '$port' -NoNewWindow $redirect"
done