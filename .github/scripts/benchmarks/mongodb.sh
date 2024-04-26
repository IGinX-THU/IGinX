#!/bin/sh
if [ "$RUNNER_OS" = "Linux" ]; then
  set -e
  sh -c "wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-6.0.4.tgz"
  sh -c "tar -zxf mongodb-linux-x86_64-ubuntu2204-6.0.4.tgz"
  sudo sh -c "cp -r mongodb-linux-x86_64-ubuntu2204-6.0.4/ mongodb-linux-x86_64-ubuntu2204-6.0.4-27017/"
  sudo sh -c "cd mongodb-linux-x86_64-ubuntu2204-6.0.4-27017/; mkdir -p data/db; mkdir -p data/log; nohup ./bin/mongod --port 27017 --dbpath data/db --logpath data/log/mongo.log &"
elif [ "$RUNNER_OS" = "Windows" ]; then
  set -e
  echo "Downloading zip archive. This may take a few minutes..."
  sh -c "curl -LJO https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-6.0.12.zip -o mongodb-windows-x86_64-6.0.12.zip"
  sh -c "unzip -qq mongodb-windows-x86_64-6.0.12.zip"
  echo "Download finished."
  sh -c "ls mongodb-win32-x86_64-windows-6.0.12/bin"
  sh -c "cp -r mongodb-win32-x86_64-windows-6.0.12/ mongodb-win32-x86_64-windows-6.0.12-27017/"
  sh -c "cd mongodb-win32-x86_64-windows-6.0.12-27017/; mkdir -p data/db; mkdir -p logs; "
  filePrefix="mongodb-win32-x86_64-windows-6.0.12-27017"
  arguments="-ArgumentList '--port', '27017', '--dbpath', '$filePrefix/data/db', '--logpath', '$filePrefix/logs/db.log'"
  powershell -command "Start-Process -FilePath '$filePrefix/bin/mongod' $arguments"
elif [ "$RUNNER_OS" = "macOS" ]; then
  set -e
  sh -c "wget https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-6.0.4.tgz"
  sh -c "tar -zxf mongodb-macos-x86_64-6.0.4.tgz"
  sudo sh -c "cp -r mongodb-macos-x86_64-6.0.4/ mongodb-macos-x86_64-6.0.4-27017/"
  sudo sh -c "cd mongodb-macos-x86_64-6.0.4-27017/; mkdir -p data/db; mkdir -p data/log; nohup ./bin/mongod --port 27017 --dbpath data/db --logpath data/log/mongo.log &"
else
  echo "$RUNNER_OS is not supported"
  exit 1
fi