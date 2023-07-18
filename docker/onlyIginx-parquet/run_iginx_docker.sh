#!/bin/bash

# workdir: IGINX_HOME/docker/onlyiginx-parquet
# work descripition:
#   - cast iginx port and parquet ports
#   - docker run with env params

usageHint="Usage: ./run_iginx_docker.sh\n\
          -n container_name\n\
          -h host_ip\n\
          -p host port for IGinX container to cast\n\
          [optional]-o overlay network to use\n\
          -u usage hint\n"


name=null
ip=null
hostPort=null
network=null

declare -i flag=3

# read args from user
# -p IGinX-port -d parquet-port
while getopts ":n:h:p:o:u" opt
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
    h)
		if [ "${ip}" != "null" ]; then
			echo "Error: Only one host ip is needed."
      printf "${usageHint}"
			exit 1
		fi
		ip=${OPTARG}
    echo "Using host ip ${ip}."
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
    o)
		if [ "${network}" != "null" ]; then
			echo "Error: Only one overlay network is needed."
      printf "${usageHint}"
			exit 1
		fi
		network=${OPTARG}
    echo "Using overlay network ${network}."
    ;;
    u)
		printf "${usageHint}"
    exit 1
    ;;
    ?)
    echo "Error: Unknown params."
    printf "${usageHint}"
    exit 1;;
  esac
done

if (( ${flag} != 0 )); then
  echo "Error: first 3 params needed at least:"
  printf "${usageHint}"
  exit 1
fi

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
  if [[ ${item} =~ [=]?host\.docker\.internal# ]]; then
    parquetPort=$(awk -F'#' '{print $2}' <<< ${item})
    params=${params}" -p ${parquetPort}:${parquetPort}"
  fi
done

# echo "${params}"

if [ "${network}" != "null" ]; then
  network="--net=${network} "
else
  network=""
fi

command="docker run --name=${name} -h iginx_p_22 --add-host=host.docker.internal:host-gateway ${network}--privileged -dit -e ip=${ip} -e host_iginx_port=${hostPort}${params} iginx:0.6.0"
echo "RUNNING ${command}"
${command}