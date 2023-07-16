#!/bin/bash

# workdir: IGINX_HOME/docker/onlyiginx-parquet
# work descripition:
#   - cast iginx port and parquet ports
#   - docker run with env params

if (( $# != 3 )); then
  echo "Error: 3 params needed:"
  echo "  1. container name"
  echo "  2. container ip"
  echo "  3. host port for IGinX container to cast"
  exit 1
fi

name=$1
ip=$2
hostPort=$3
params=""

# read config file
configFile="../../conf/config.properties"

# find container iginx port
containerPort=$(awk -F'=' '{print $2}' <<< $(sed -n "/^port=/p" ${configFile}))
# echo "${containerPort}"
params=${params}" -p ${hostPort}:${containerPort}"

# find container parquet port
engineListStr=$(sed -n "/^storageEngineList=/p" ${configFile})
engineListArr=($(echo ${engineListStr} | tr "," "\n"))
for item in "${engineListArr[@]}"; do
  parquetPort=$(awk -F'#' '{print $2}' <<< ${item})
  params=${params}" -p ${parquetPort}:${parquetPort}"
done

# echo "${params}"

command="docker run --name=${name} --add-host=host.docker.internal:host-gateway --privileged -dit -e ip=${ip} -e host_iginx_port=${hostPort}${params} iginx:0.6.0"
echo "RUNNING ${command}"
${command}