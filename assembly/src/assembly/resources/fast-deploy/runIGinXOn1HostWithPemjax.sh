#!/bin/bash
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


fail() {
    printf 'ERROR: %s\nStartup aborted.\n' "$1" >&2
    exit 1
}

# This wrapper launches the distribution containing this script.
basepath=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
export IGINX_HOME="$basepath"
export IGINX_CONF_DIR="$IGINX_HOME/conf"
export IGINX_CONF="$IGINX_CONF_DIR/config.properties"
[[ -r "$IGINX_CONF" ]] || fail "Cannot read $IGINX_CONF"
if [[ -f "$IGINX_CONF_DIR/iginx-env.sh" ]]; then
    source "$IGINX_CONF_DIR/iginx-env.sh" || fail 'Cannot load iginx-env.sh.'
fi
[[ -x "$IGINX_HOME/include/apache-zookeeper/bin/zkServer.sh" ]] ||
    fail 'The ZooKeeper startup script is missing or not executable.'
[[ -x "$IGINX_HOME/sbin/start_iginx.sh" ]] ||
    fail 'sbin/start_iginx.sh is missing or not executable.'

# Read only the requested property, ignoring comments; the last definition wins.
read_property() {
    awk -v key="$1" -v fallback="$2" '
        BEGIN { value = fallback }
        {
            sub(/\r$/, "")
            pattern = "^[ \t]*" key "([ \t]*[=:][ \t]*|[ \t]+)"
            if ($0 ~ pattern) {
                sub(pattern, "")
                value = $0
            }
        }
        END { print value }
    ' "$IGINX_CONF"
}

python_exec=$(read_property pythonCMD python3) || fail 'Cannot read pythonCMD.'
use_env=$(read_property enableEnvParameter false) || fail 'Cannot read enableEnvParameter.'
if [[ "${use_env,,}" == true && -n "${pythonCMD:-}" ]]; then
    python_exec="$pythonCMD"
else
    # Avoid interpreting Java-property escapes differently from IGinX.
    [[ "$python_exec" != *\\* ]] || fail 'Use a path without backslash escapes for pythonCMD.'
fi

# Match IGinX's working directory, also for relative interpreter paths.
cd -- "$IGINX_HOME/sbin" || fail 'Cannot enter the sbin directory.'
"$python_exec" -c "import sys; print('IGinX Python: ' + sys.executable)" ||
    fail 'pythonCMD must name a working Python executable in conf/config.properties.'

# Installation and verification must both finish before either service starts.
"$python_exec" -m pip install pemjax || fail 'pemjax installation failed.'
"$python_exec" -c "import importlib.metadata as metadata; import pemja; print('Pemjax ' + metadata.version('pemjax') + ' verified.')" ||
    fail 'pemjax is not usable in the configured Python environment.'

(
    cd -- "$IGINX_HOME/include/apache-zookeeper" && bin/zkServer.sh start
) || fail 'ZooKeeper startup failed.'
printf 'ZooKeeper startup requested.\n'

nohup ./start_iginx.sh > nohup.out 2>&1 < /dev/null &
iginx_pid=$!
printf 'IGinX startup requested (PID %s). Check sbin/nohup.out for readiness.\n' "$iginx_pid"
