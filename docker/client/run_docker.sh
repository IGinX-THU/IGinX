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

datadir="$(pwd)/data"
name="iginx-client"

while [ "$1" != "" ]; do
    case $1 in
        -n ) shift
             name=$1
             ;;
        --datadir )
            shift
            datadir="$1"
            ;;
        * )
            echo "Invalid option: $1"
            exit 1
            ;;
    esac
    shift
done

[ -d "$datadir" ] || mkdir -p "$datadir"

command="docker run --name=\"$name\" --privileged -dit --add-host=host.docker.internal:host-gateway --mount type=bind,source=${datadir},target=/iginx_client/data iginx-client:0.8.0"
echo $command
eval $command
