#!/bin/bash

set -e

cp -r test/src/test/resources/udf docker/client/data

# 找到docker nat adapter的ip4地址
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


export MSYS_NO_PATHCONV=1
# MSYS_NO_PATHCONV=1 : not to convert docker script path to git bash path
docker exec iginx-client powershell -Command "Get-ChildItem -Path C:/iginx_client/sbin"
docker exec iginx-client powershell -Command "Get-ChildItem -Path C:/iginx_client/data"
# test java and network
docker exec iginx-client powershell -Command "java -version"
docker exec iginx-client powershell -Command "ping ${trimmed_string}"

docker exec iginx-client cmd /c "C:/iginx_client/sbin/start_cli.bat" -h "${trimmed_string}" -e 'show functions;'
docker exec iginx-client cmd /c "C:/iginx_client/sbin/start_cli.bat" -h "${trimmed_string}" -e 'create function udtf "mock_udf" from "MockUDF" in "C:/iginx_client/data/udf/mock_udf.py";'
docker exec iginx-client cmd /c "C:/iginx_client/sbin/start_cli.bat" -h "${trimmed_string}" -e 'CREATE FUNCTION udtf "udf_a" FROM "my_module.my_class_a.ClassA", "udf_b" FROM "my_module.my_class_a.ClassB", "udf_sub" FROM "my_module.sub_module.sub_class_a.SubClassA" IN "C:/iginx_client/data/udf/my_module";'
docker exec iginx-client cmd /c "C:/iginx_client/sbin/start_cli.bat" -h "${trimmed_string}" -e 'CREATE FUNCTION udtf "udf_a_file" FROM "ClassA", udsf "udf_b_file" FROM "ClassB", udaf "udf_c_file" FROM "ClassC" IN "../data/udf/my_module/idle_classes.py";'
docker exec iginx-client cmd /c "C:/iginx_client/sbin/start_cli.bat" -h "${trimmed_string}" -e 'show functions;'
