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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
 

set -e

cp -r test/src/test/resources/udf docker/client/data

ls docker/client/data
ls docker/client/data/udf

set os=$1
echo "$os"

ifconfig

results=$(ifconfig)

adapterfound=false
trimmed_string=notFound

# 逐行读取 ifconfig 输出
while IFS= read -r line; do
    if [[ $line =~ "en0:" ]]; then
        adapterfound=true
    elif [[ "${adapterfound}" == "true" && $line =~ "inet " ]]; then
        IFS=' '
        read -ra arr <<< "$line"
        trimmed_string=$(echo ${arr[1]} | tr -d ' ')
        echo $trimmed_string
        adapterfound=false
    fi
done <<< "$results"

if [[ "$line" == "notFound" ]]; then
    echo "ip4 addr for host not found"
    exit 1
fi

export MSYS_NO_PATHCONV=1
# MSYS_NO_PATHCONV=1 : not to convert docker script path to git bash path
SCRIPT_PREFIX="docker exec iginx-client /iginx_client/sbin/start_cli.sh -h ${trimmed_string} -e"

# single udf in one file
${SCRIPT_PREFIX} "create function udtf \"mock_udf\" from \"MockUDF\" in \"/iginx_client/data/udf/mock_udf.py\";"
# multiple udfs in one module
${SCRIPT_PREFIX} "CREATE FUNCTION udtf \"udf_a\" FROM \"my_module.my_class_a.ClassA\", \"udf_b\" FROM \"my_module.my_class_a.ClassB\", \"udf_sub\" FROM \"my_module.sub_module.sub_class_a.SubClassA\" IN \"/iginx_client/data/udf/my_module\";"
# multiple udfs in one file
${SCRIPT_PREFIX} "CREATE FUNCTION udtf \"udf_a_file\" FROM \"ClassA\", udsf \"udf_b_file\" FROM \"ClassB\", udaf \"udf_c_file\" FROM \"ClassC\" IN \"/iginx_client/data/udf/my_module/idle_classes.py\";"

${SCRIPT_PREFIX} "show functions;"
