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

cd core/target/iginx-core-$1/

iginx_home_path=$PWD

cd ..

if [ -n "$MSYSTEM" ]; then
    windows_path=$(cygpath -w "$iginx_home_path")

    export IGINX_HOME=$windows_path

    powershell -Command "Start-Process -FilePath 'iginx-core-$1/sbin/start_iginx.bat' -NoNewWindow -RedirectStandardOutput '../../iginx-udf.log' -RedirectStandardError '../../iginx-udf-error.log'"
else
    export IGINX_HOME=$iginx_home_path

    sh -c "chmod +x iginx-core-$1/sbin/start_iginx.sh"

    sh -c "nohup iginx-core-$1/sbin/start_iginx.sh > ../../iginx-u.log 2>&1 &"
fi
