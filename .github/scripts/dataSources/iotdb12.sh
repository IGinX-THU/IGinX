#!/bin/sh
# 循环处理命令行参数中的每个整数值
for port in "$@"
do
  set -e
  if test -e "apache-iotdb-0.12.6-server-bin.zip"; then
    echo "File already exists, skipping download."
  else
    sh -c "wget -nv https://github.com/thulab/IginX-benchmarks/raw/main/resources/apache-iotdb-0.12.6-server-bin.zip"

    sh -c "unzip -qq apache-iotdb-0.12.6-server-bin.zip"

    sh -c "sleep 10"

    sh -c "ls ./"

    sh -c "echo ========================="

    sh -c "ls apache-iotdb-0.12.6-server-bin"

    sh -c "sudo sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties"

  fi
  sh -c "sudo cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb2-0.12.6-server-bin-$port"

  sh -c "sudo sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb2-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sh -c "sudo sed -i 's/6667/$port/g' apache-iotdb2-0.12.6-server-bin-$port/conf/iotdb-engine.properties"

  sudo sh -c "cd apache-iotdb2-0.12.6-server-bin-$port/; nohup sbin/start-server.sh &"

done
