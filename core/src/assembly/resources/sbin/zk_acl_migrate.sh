#!/usr/bin/env bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

set -e
if [[ -z "${IGINX_HOME:-}" ]]; then
  export IGINX_HOME=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
fi
export IGINX_CONF="${IGINX_CONF:-${IGINX_CONF_DIR:-$IGINX_HOME/conf}/config.properties}"
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
exec "$JAVA" -cp "$IGINX_HOME/conf:$IGINX_HOME/lib/*" cn.edu.tsinghua.iginx.metadata.storage.zk.ZooKeeperAclMigration "$@"
