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


# usage:.sh <port> <old_password> <new_password>

set -e

cd apache-iotdb-0.12.6-server-bin-$1/
sh -c "sbin/start-cli.bat -h 127.0.0.1 -p $1 -u root -pw $2 -e 'ALTER USER root SET PASSWORD \"$3\";'"