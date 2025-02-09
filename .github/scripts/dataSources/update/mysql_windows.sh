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

set -e

if [ $2 = "set" ]; then
    # set password when there was none
    mysql -h 127.0.0.1 --port=$1 -u root -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '$3'; flush privileges;"
else
    # remove password
    mysql -h 127.0.0.1 --port=$1 -u root -p$3 -e "ALTER USER 'root'@'localhost' IDENTIFIED BY ''; flush privileges;"
fi
