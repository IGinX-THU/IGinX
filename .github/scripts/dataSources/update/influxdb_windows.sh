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


# usage:.sh <target_port> <new_org_id>

set -e

cd influxdb2-2.0.7-windows-amd64/

# 所有org的信息
output=$(influx org list)

echo $output

# 只有一个组织，所以直接匹配
id=$(echo "$output" | grep -Eo '^[a-z0-9]{16}')

# 验证
echo "Extracted ID: $id"

sh -c "./influx org --host http://localhost:$1 -t testToken update -i $id -n $2"
