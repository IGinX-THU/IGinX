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

# usage:.sh <port> <mode>:set/unset <password>
CONTAINER_NAME="oracle23-$1"  # 容器名称
DATABASE_USER="system"

if [ $2 = "set" ]; then
    # set password when there was none
    docker exec -it $CONTAINER_NAME bash -c "
      sqlplus -s / as sysdba <<EOF
      ALTER USER $DATABASE_USER IDENTIFIED BY $3;
      EXIT;
    EOF"
else
   docker exec -it $CONTAINER_NAME bash -c "
         sqlplus -s / as sysdba <<EOF
         ALTER USER $DATABASE_USER IDENTIFIED BY NULL;
         EXIT;
       EOF"
fi