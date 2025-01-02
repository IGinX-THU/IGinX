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

PORT=$1
current_dir=$(pwd)
cd "$SERVICE_DIR_WIN/redis"
redis-server --port $PORT --dir "$SERVICE_DIR_WIN/redis/$PORT" --daemonize yes --pidfile "$SERVICE_DIR_WIN/redis/$PORT/redis.pid"
cd $current_dir

