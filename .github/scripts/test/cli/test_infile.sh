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

COMMAND='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, d, b, c, a);'

SCRIPT_COMMAND="bash client/target/iginx-client-$1/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-$1/sbin/start_cli.sh"

bash -c "echo '$COMMAND' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "cat 'test/src/test/resources/fileReadAndWrite/csv/test.csv' > 'test/src/test/resources/fileReadAndWrite/csv/test1'"

bash -c "sed -i '1ikey,d m,b,[c],a' 'test/src/test/resources/fileReadAndWrite/csv/test1'"

COMMAND1='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test1"'" AS CSV INTO t1;'

bash -c "echo '$COMMAND1' | xargs -0 -t -i ${SCRIPT_COMMAND}"
