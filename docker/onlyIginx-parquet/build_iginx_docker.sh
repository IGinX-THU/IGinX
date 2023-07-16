#!/bin/bash

# workdir: IGINX_HOME/docker/onlyiginx-parquet
# work descripition:
#   - change 0 or 1 iginx port in config acccording to Command line arguments -p (port)
#   - change 0 or n parquet ports in config acccording to Command line arguments -d (database)
#   - change 127.0.0.1 to host.docker.internal for container to access host
#   - enable env params in config
#   - docker build

IGinXPort=-1
parquetPorts=()
declare -i parquetPortsCount=0
usageHint="Usage: ./build_iginx_docker.sh [-p IGinX-port] {-d parquet-port}"

# read args from user
# -p IGinX-port -d parquet-port
while getopts ":p:d:" opt
do
  case $opt in
    p)
		if [ ${IGinXPort} != -1 ]; then
			echo "Error: Only one IGinX Port is needed."
      echo "${usageHint}"
			exit 1
		fi
		IGinXPort=${OPTARG}
    echo "Using IGinX port ${IGinXPort}."
    # echo "IGinXPort=${IGinXPort}"
    ;;
    d)
		parquetPorts[${parquetPortsCount}]=${OPTARG}
		parquetPortsCount+=1
    echo "Using Parquet port ${parquetPortsCount} ${OPTARG}."
    # echo "${parquetPortsCount}.${parquetPorts[${parquetPortsCount}-1]}"
    ;;
    ?)
    echo "Error: Unknown params."
    echo "${usageHint}"
    exit 1;;
  esac
done

# read config file
configFile="../../conf/config.properties"

# change parquet port
if (( ${parquetPortsCount} != 0 )); then
  engineListStr=$(sed -n "/^storageEngineList=/p" ${configFile})
  engineListArr=($(echo ${engineListStr} | tr "," "\n"))
  if (( ${parquetPortsCount} != ${#engineListArr[*]} )); then
    echo "Error: You should set all ${#engineListArr[*]} parquet ports or none."
    echo "${usageHint}"
    exit 1
  fi
fi

# change iginx ports
if (( ${IGinXPort} != -1 )); then
  portConfig=$(sed -n "/^port=/p" ${configFile})
  sed -i "s/${portConfig}/port=${IGinXPort}/g" ${configFile}
fi

# enable env params
sed -i "s/$(sed -n "/^enableEnvParameter=/p" ${configFile})/enableEnvParameter=true/g" ${configFile}

# change host address
sed -i "s/127.0.0.1/host.docker.internal/g" ${configFile}

command="docker build --network=host --file Dockerfile-iginx -t iginx:0.6.1 ../.."
echo "RUNNING ${command}"
${command}
