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


echo "正在停止端口 $PORT 上的InfluxDB进程..."
if [ "$OS" == "Windows" ]; then
    tasklist //FI "IMAGENAME eq influxd.exe" //FI "WINDOWTITLE eq *:$PORT*" //FO CSV //NH | findstr influxd.exe > nul
    if [ $? -eq 0 ]; then
        taskkill //F //FI "IMAGENAME eq influxd.exe" //FI "WINDOWTITLE eq *:$PORT*"
    else
        echo "未找到在端口 $PORT 上运行的InfluxDB进程"
        return 1
    fi
else
    pkill -f "influxd.*:$PORT"
fi
sleep 10
netstat -ano | grep ":$1"

