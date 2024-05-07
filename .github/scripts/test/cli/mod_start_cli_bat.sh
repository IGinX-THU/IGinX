#!/bin/bash

set -e

cp -r test/src/test/resources/udf docker/client/data

# 找到docker nat的ip4地址
readarray -t results < <(ipconfig)
adapterfound=false
trimmed_string=notFound

for line in "${results[@]}"; do
	if [[ $line =~ "vEthernet (nat)" ]]; then
		adapterfound=true
	elif [[ "${adapterfound}" == "true" && $line =~ "IPv4" ]]; then
		IFS=':'
		read -ra arr <<< "$line"
		trimmed_string=$(echo ${arr[1]} | tr -d ' ')
		echo $trimmed_string
		adapterfound=false
	fi
done

if [[ "$line" == "notFound" ]]; then
    echo "ip4 addr for host not found"
    exit 1
fi

sed -i "s/^set h_parameter=.*$/set h_parameter=-h ${trimmed_string}/g" client/src/assembly/resources/sbin/start_cli.bat