#!/bin/sh
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

sh -c "chmod +x .github/scripts/dataSources/startup/iotdb12.sh"

sh -c "chmod +x .github/scripts/dataSources/startup/influxdb.sh"

sh -c ".github/scripts/dataSources/startup/iotdb12.sh 6667"

sh -c ".github/scripts/dataSources/startup/influxdb.sh"

set -i "s/storageEngineList/#storageEngineList/g" conf/config.properties

echo "storageEngineList=127.0.0.1#6667#iotdb12#username=root#password=root#sessionPoolSize=50#has_data=false#is_read_only=false,127.0.0.1#8086#influxdb#url=http://localhost:8086/#token=testToken#organization=testOrg#has_data=false" >> conf/config.properties
