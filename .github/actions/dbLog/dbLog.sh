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
 

# Only works on WindowsOS
# all logs are stored in {DB_DIR_ROOT}/logs/db.log & {DB_DIR_ROOT}/logs/db-error.log(optional)
dbName=$1

case $dbName in
  IoTDB12)
    dirName=apache-iotdb-0.12.6-server-bin-*
    ;;
  InfluxDB)
    dirName=influxdb2-2.7.4-window*
    ;;
  MongoDB)
    dirName=mongodb-win32-x86_64-windows-6.0.12-*
    ;;
  Redis)
    dirName=Redis-7.0.14-Windows-x64-*
    ;;
  PostgreSQL)
    dirName=pgsql-*
    ;;
  *)
    echo "DB:$dbName log not supported."
    exit 0
    ;;
esac

for dir in $dirName; do
    if [ -d "$dir" ]; then
        echo "Entering: $dir"

        # show db.log & db-error.log
        if [ -f "$dir/logs/db.log" ]; then
            echo "cat $dir/logs/db.log :"
            cat "$dir/logs/db.log"
        else
            echo "$dir/logs/db.log not found."
        fi

        if [ -f "$dir/logs/db-error.log" ]; then
            echo "cat $dir/logs/db-error.log :"
            cat "$dir/logs/db-error.log"
        else
            echo "$dir/logs/db-error.log not found."
        fi
    fi
done

