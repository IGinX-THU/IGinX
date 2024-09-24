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

cp -f test/src/test/resources/udf/mock_udf.py client/target/iginx-client-$1/sbin/mock_udf.py

ls client/target/iginx-client-$1/sbin

COMMAND='CREATE FUNCTION UDAF "'"mock_udf"'" FROM "'"MockUDF"'" IN "'"mock_udf.py"'";'

cd client/target/iginx-client-$1/sbin

result=$(bash -c "./start_cli.bat -e '$COMMAND'")

if [[ $result =~ 'success' ]]; then
  echo success
  COMMAND='DROP FUNCTION "'"mock_udf"'";'
  bash -c "./start_cli.bat -e '$COMMAND'"
else
  echo 'Error: failed to register udf mock_udf.'
  echo $result
  exit 1
fi
