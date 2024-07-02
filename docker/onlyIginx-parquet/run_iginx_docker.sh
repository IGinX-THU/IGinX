#!/bin/bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#


# workdir: $IGINX_HOME/docker/onlyiginx-parquet
# work descripition:
#   - cast iginx port and parquet ports to host
#   - docker run with params

usageHint="Usage: ./run_iginx_docker.sh\n\
          -n container_name\n\
          -p host port for IGinX container to cast\n\
          [optional]-c absolute path of local config file (default: "../../conf/config.properties")\n\
          [optional]-o network to use (default: bridge)\n\
          -u usage hint\n"


name=null
hostPort=null
network=null
localConfigFile=null

declare -i flag=2

# read args from user
while getopts ":n:p:o:c:u" opt
do
  case $opt in
    n)
		if [ "${name}" != "null" ]; then
			echo "Error: Only one container name is needed."
      printf "${usageHint}"
			exit 1
		fi
		name=${OPTARG}
    echo "Using container name ${name}."
    flag+=-1
    ;;
    p)
		if [ "${hostPort}" != "null" ]; then
			echo "Error: Only one host port is needed."
      printf "${usageHint}"
			exit 1
		fi
		hostPort=${OPTARG}
    echo "Using host port ${hostPort}."
    flag+=-1
    ;;
    c)
		if [ "${localConfigFile}" != "null" ]; then
			echo "Error: Only one local config file is needed."
      printf "${usageHint}"
			exit 1
		fi
		localConfigFile=${OPTARG}
    echo "Using local config file: ${localConfigFile}."
    ;;
    o)
		if [ "${network}" != "null" ]; then
			echo "Error: Only one network param is needed."
      printf "${usageHint}"
			exit 1
		fi
		network=${OPTARG}
    echo "Using overlay network ${network}."
    ;;
    u)
		printf "${usageHint}"
    exit 0
    ;;
    ?)
    echo "Error: Unknown params."
    printf "${usageHint}"
    exit 1;;
  esac
done

if (( ${flag} != 0 )); then
  echo "Error: first 2 params must be provided:"
  printf "${usageHint}"
  exit 1
fi

portCastParams=""

# read config file. default:"../../conf/config.properties"
if [[ "${localConfigFile}" == "null" ]]; then
  localConfigFile="$(dirname $(dirname "$PWD"))/conf/config.properties"
  echo "Using local config file: ${localConfigFile}"
fi
if [ ! -f ${localConfigFile} ]; then
  echo "File ${localConfigFile} does not exist"
  exit 1
fi


# find container IGinX port and cast
containerPort=$(awk -F'=' '{print $2}' <<< $(sed -n "/^port=/p" ${localConfigFile}))
portCastParams=${portCastParams}" -p ${hostPort}:${containerPort}"

# find container IGinX IP
containerIP=$(awk -F'=' '{print $2}' <<< $(sed -n "/^ip=/p" ${localConfigFile}))

# find container parquet port, only cast local parquet ports.
# (IGinX ip == parquet ip) ? local : remote
engineListStr=$(sed -n "/^storageEngineList=/p" ${localConfigFile})
engineListArr=($(echo ${engineListStr} | tr "," "\n"))
for item in "${engineListArr[@]}"; do
  if [[ ${item} =~ (=|^)${containerIP}# ]]; then
    parquetPort=$(awk -F'#' '{print $2}' <<< ${item})
    portCastParams=${portCastParams}" -p ${parquetPort}:${parquetPort}"
  fi
done

# read network ip from config file if network is specified
if [ "${network}" != "null" ]; then
  network="--net=${network} "
  localIPConfig="--ip=${containerIP} "
else
  network=""
  localIPConfig=""
fi

configFileConfig="-v ${localConfigFile}:/iginx/conf/config.properties "

command="docker run --name=${name} ${localIPConfig}${configFileConfig}--add-host=host.docker.internal:host-gateway ${network}--privileged -dit -e host_iginx_port=${hostPort}${portCastParams} iginx:0.7.0-SNAPSHOT"
echo "RUNNING ${command}"
# ${command}