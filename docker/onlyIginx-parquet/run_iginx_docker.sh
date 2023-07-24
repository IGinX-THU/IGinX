#!/bin/bash

# workdir: $IGINX_HOME/docker/onlyiginx-parquet
# work descripition:
#   - cast iginx port and parquet ports to host
#   - docker run with params

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
  echo "Error: first 3 params are not optional:"
  printf "${usageHint}"
  exit 1
fi

portCastParams=""

# read config file
configFile="../../conf/config.properties"

# find container iginx port
containerPort=$(awk -F'=' '{print $2}' <<< $(sed -n "/^port=/p" ${configFile}))
portCastParams=${portCastParams}" -p ${hostPort}:${containerPort}"

# find container parquet port, only cast local parquet ports
engineListStr=$(sed -n "/^storageEngineList=/p" ${configFile})
engineListArr=($(echo ${engineListStr} | tr "," "\n"))
for item in "${engineListArr[@]}"; do
  if [[ ${item} =~ "#isLocal=true" ]]; then
    parquetPort=$(awk -F'#' '{print $2}' <<< ${item})
    portCastParams=${portCastParams}" -p ${parquetPort}:${parquetPort}"
  fi
done

# read network ip from config file if network is specified
if [ "${network}" != "null" ]; then
  network="--net=${network} "
  localIP="--ip=$(awk -F'=' '{print $2}' <<< $(sed -n "/^ip=/p" ${configFile})) "
else
  network=""
  localIP=""
fi

command="docker run --name=${name} ${localIP}--add-host=host.docker.internal:host-gateway ${network}--privileged -dit -e ip=${ip} -e host_iginx_port=${hostPort}${portCastParams} iginx:0.6.0"
echo "RUNNING ${command}"
${command}