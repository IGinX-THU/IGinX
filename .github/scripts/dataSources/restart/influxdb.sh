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

# usage:.sh <target_port>


if [ $# -eq 0 ]; then
    echo "需要提供端口"
    exit 1
fi

PORT=$1

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="Linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macOS"
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    OS="Windows"
else
    echo "无法检测操作系统类型"
    exit 1
fi

INFLUXDB_PATH="influxdb2-2.0.7-linux-amd64-$PORT"
if [ "$OS" == "macOS" ]; then
    INFLUXDB_PATH="influxdb2-2.0.7-darwin-amd64-$PORT"
elif [ "$OS" == "Windows" ]; then
    INFLUXDB_PATH="influxdb2-2.0.7-windows-amd64-$PORT"
fi

# 启动
if [ "$OS" == "Windows" ]; then
    powershell -command "Start-Process -FilePath '$INFLUXDB_PATH/influxd' -ArgumentList 'run', '--bolt-path=$INFLUXDB_PATH/.influxdbv2/influxd.bolt', '--engine-path=$INFLUXDB_PATH/.influxdbv2/engine', '--http-bind-address=:$PORT', '--query-memory-bytes=20971520' -NoNewWindow -RedirectStandardOutput '$INFLUXDB_PATH/logs/db.log' -RedirectStandardError '$INFLUXDB_PATH/logs/db-error.log'"
else
    nohup $INFLUXDB_PATH/influxd run --bolt-path=$INFLUXDB_PATH/.influxdbv2/influxd.bolt --engine-path=$INFLUXDB_PATH/.influxdbv2/engine --http-bind-address=:$PORT --query-memory-bytes=20971520 > $INFLUXDB_PATH/logs/db.log 2> $INFLUXDB_PATH/logs/db-error.log &
fi

sleep 10
lsof -i:$PORT