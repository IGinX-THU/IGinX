#!/bin/bash

set -e

cp -r test/src/test/resources/udf docker/client/data

ls docker/client/data
ls docker/client/data/udf

set os=$1
echo "$os"

export MSYS_NO_PATHCONV=1
# MSYS_NO_PATHCONV=1 : not to convert docker script path to git bash path
docker exec iginx-client powershell -Command "Get-ChildItem -Path C:/iginx_client/sbin"
docker exec iginx-client powershell -Command "Get-ChildItem -Path C:/iginx_client/data"
docker exec iginx-client powershell -command "Start-Process  -NoNewWindow -FilePath 'C:/iginx_client/sbin/start_cli.bat' -ArgumentList '-h host.docker.internal -e \"show cluster info;\"'"
docker exec iginx-client powershell -command "Start-Process  -NoNewWindow -FilePath 'C:/iginx_client/sbin/start_cli.bat' -ArgumentList '-e \"create function udtf \\\"mock_udf\\\" from \\\"MockUDF\\\" in \\\"C:/iginx_client/data/udf/mock_udf.py\\\";\"'"
docker exec iginx-client powershell -command "Start-Process  -NoNewWindow -FilePath 'C:/iginx_client/sbin/start_cli.bat' -ArgumentList '-e \"CREATE FUNCTION udtf \\\"udf_a\\\" FROM \\\"my_module.my_class_a.ClassA\\\", \\\"udf_b\\\" FROM \\\"my_module.my_class_a.ClassB\\\", \\\"udf_sub\\\" FROM \\\"my_module.sub_module.sub_class_a.SubClassA\\\" IN \\\"C:/iginx_client/data/udf/my_module\\\";\"'"
docker exec iginx-client powershell -command "Start-Process  -NoNewWindow -FilePath 'C:/iginx_client/sbin/start_cli.bat' -ArgumentList '-e \"CREATE FUNCTION udtf \\\"udf_a_file\\\" FROM \\\"ClassA\\\", udsf \\\"udf_b_file\\\" FROM \\\"ClassB\\\", udaf \\\"udf_c_file\\\" FROM \\\"ClassC\\\" IN \\\"C:/iginx_client/data/udf/my_module/idle_classes.py\\\";\"'"
docker exec iginx-client powershell -command "Start-Process  -NoNewWindow -FilePath 'C:/iginx_client/sbin/start_cli.bat' -ArgumentList '-e \"show functions;\"'"

docker logs iginx-client

#$SCRIPT_PREFIX1
#echo $SCRIPT_PREFIX1
#$SCRIPT_PREFIX2
#echo $SCRIPT_PREFIX2

#SCRIPT_PREFIX="docker exec iginx-client cmd /c 'C:\\iginx_client\\sbin\\start_cli.bat -h host.docker.internal -e "
#echo $SCRIPT_PREFIX "\"create function udtf \\\"mock_udf\\\" from \\\"MockUDF\\\" in \\\"../data/udf/mock_udf.py\\\";\"'"

#docker exec container_name cmd /c "C:\\iginx_client\\sbin\\start_cli.bat -h host.docker.internal -e \"create function udtf \\\"mock_udf\\\" from \\\"MockUDF\\\" in \\\"../data/udf/mock_udf.py\\\";\""


#sleep 5
##
##docker ps
##docker network inspect docker-cluster-iginx
##ls logs/docker_logs
#cat logs/*
##docker exec iginx0 cat /logs/iginx-latest.log
##
#timeout=30
#interval=2
#
#elapsed_time=0
#while [ $elapsed_time -lt $timeout ]; do
#  output=$(${SCRIPT_PREFIX} "show cluster info;")
#  if [[ $output =~ 'Connection refused (Connection refused)' ]]; then
#      echo "$output"
#      sleep $interval
#  else
#      break
#  fi
#  elapsed_time=$((elapsed_time + interval))
#done
#if [[ $output =~ 'Connection refused (Connection refused)' ]]; then
#  echo "IGinX not reachable"
#  exit 1
#fi
#
#create function udtf "mock_udf" from "MockUDF" in "../data/udf/mock_udf.py";
#
## single udf in one file
#echo ${SCRIPT_PREFIX} "\"create function udtf \\\"mock_udf\\\" from \\\"MockUDF\\\" in \\\"../data/udf/mock_udf.py\\\";\"'"
#${SCRIPT_PREFIX} "\"create function udtf \\\"mock_udf\\\" from \\\"MockUDF\\\" in \\\"../data/udf/mock_udf.py\\\";\"'"
## multiple udfs in one module
#${SCRIPT_PREFIX} "\\\"CREATE FUNCTION udtf \"udf_a\" FROM \"my_module.my_class_a.ClassA\", \"udf_b\" FROM \"my_module.my_class_a.ClassB\", \"udf_sub\" FROM \"my_module.sub_module.sub_class_a.SubClassA\" IN \"../data/udf/my_module\";\\\""
#echo ${SCRIPT_PREFIX} "\\\"CREATE FUNCTION udtf \"udf_a\" FROM \"my_module.my_class_a.ClassA\", \"udf_b\" FROM \"my_module.my_class_a.ClassB\", \"udf_sub\" FROM \"my_module.sub_module.sub_class_a.SubClassA\" IN \"../data/udf/my_module\";\\\""
## multiple udfs in one file
#${SCRIPT_PREFIX} "\\\"CREATE FUNCTION udtf \"udf_a_file\" FROM \"ClassA\", udsf \"udf_b_file\" FROM \"ClassB\", udaf \"udf_c_file\" FROM \"ClassC\" IN \"../data/udf/my_module/idle_classes.py\";\\\""
#echo ${SCRIPT_PREFIX} "\\\"CREATE FUNCTION udtf \"udf_a_file\" FROM \"ClassA\", udsf \"udf_b_file\" FROM \"ClassB\", udaf \"udf_c_file\" FROM \"ClassC\" IN \"../data/udf/my_module/idle_classes.py\";\\\""
#
#${SCRIPT_PREFIX} "\\\"show functions;\\\""
#echo ${SCRIPT_PREFIX} "\\\"show functions;\\\""
