#!/bin/bash

set -e

cp -r test/src/test/resources/udf docker/client/data

ls docker/client/data
ls docker/client/data/udf

set os=$1
echo "$os"

export MSYS_NO_PATHCONV=1
# MSYS_NO_PATHCONV=1 : not to convert docker script path to git bash path
SCRIPT_PREFIX="docker exec iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal -e"
if [[ $os =~ "mac" ]]; then
    SCRIPT_PREFIX="docker exec iginx-client /iginx_client/sbin/start_cli.sh -h 192.168.65.1 -e"
fi

# single udf in one file
${SCRIPT_PREFIX} "create function udtf \"mock_udf\" from \"MockUDF\" in \"/iginx_client/data/udf/mock_udf.py\";"
# multiple udfs in one module
${SCRIPT_PREFIX} "CREATE FUNCTION udtf \"udf_a\" FROM \"my_module.my_class_a.ClassA\", \"udf_b\" FROM \"my_module.my_class_a.ClassB\", \"udf_sub\" FROM \"my_module.sub_module.sub_class_a.SubClassA\" IN \"/iginx_client/data/udf/my_module\";"
# multiple udfs in one file
${SCRIPT_PREFIX} "CREATE FUNCTION udtf \"udf_a_file\" FROM \"ClassA\", udsf \"udf_b_file\" FROM \"ClassB\", udaf \"udf_c_file\" FROM \"ClassC\" IN \"/iginx_client/data/udf/my_module/idle_classes.py\";"

${SCRIPT_PREFIX} "show functions;"
