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

port=$1

pwd

docker-compose -f $port/docker-compose-$port.yml up -d

echo "waiting for the server to start..."
sleep 10
docker ps
docker logs milvus$port-standalone
docker logs milvus$port-minio
docker logs milvus$port-etcd

if lsof -i :$port | grep -q LISTEN; then
  echo "Port $port is open."
else
  echo "Port $port is not open."
fi