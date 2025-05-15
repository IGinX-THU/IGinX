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

port=$1
pwd
echo "start neo4j $port"
powershell.exe -Command "\$env:JAVA_HOME = './jdk17';\$env:PATH = \"\$env:JAVA_HOME\\bin;\$env:PATH\";Start-Process -FilePath './neo4j_instances/$port/bin/neo4j.bat' -ArgumentList 'console' -WindowStyle Hidden"
echo "start OK."
sleep 10