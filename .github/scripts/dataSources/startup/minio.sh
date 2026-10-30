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

# usage: minio.sh <port>
# start a minio server on <port>, data is kept in ${SERVICE_DIR}/minio/<port>
set -e

PORT=$1
WORK_DIR="${SERVICE_DIR:-$(pwd)/.github/actions/service}/minio"
mkdir -p "$WORK_DIR/$PORT/data"
cd "$WORK_DIR"

MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  nohup minio server "$PORT/data" --address ":$PORT" > "$PORT/minio.log" 2>&1 &
echo $! > "$PORT/minio.pid"

for i in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:$PORT/minio/health/ready" > /dev/null; then
    echo "minio is ready on port $PORT"
    exit 0
  fi
  sleep 1
done

echo "minio failed to start on port $PORT"
cat "$PORT/minio.log"
exit 1
