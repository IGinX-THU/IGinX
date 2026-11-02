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


# Wait until a service accepts connections on every given port.
# usage: wait_for_port.sh <name> <timeout-seconds> <port> [port...]
# Uses the per-OS check in verify*.sh, so it works on Linux, macOS and Windows.

set -e

name=$1
timeout=$2
shift 2

script_dir=$(cd "$(dirname "$0")" && pwd)
case "$RUNNER_OS" in
  macOS) verify="$script_dir/verify_macos.sh" ;;
  Windows) verify="$script_dir/verify_windows.sh" ;;
  *) verify="$script_dir/verify.sh" ;;
esac
chmod +x "$verify"

for port in "$@"; do
  "$verify" "$name" "$port" "$timeout"
done
