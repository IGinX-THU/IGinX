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

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

if [ -z "${IGINX_CLI_HOME}" ]; then
  export IGINX_CLI_HOME="$(
    cd "$(dirname "$0")"/..
    pwd
  )"
fi

MAIN_CLASS=cn.edu.tsinghua.iginx.client.IginxClient

CLASSPATH=""
for f in ${IGINX_CLI_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

if [ -n "$JAVA_HOME" ]; then
  for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
    if [ -x "$java" ]; then
      JAVA="$java"
      break
    fi
  done
else
  JAVA=java
fi

PARAMETERS=("$@")

# Added parameters when default parameters are missing

# sh version
case "${PARAMETERS[@]}" in
*"-fs "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-fs" "1000" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-pw "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-pw" "root" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-u "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-u" "root" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-p "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-p" "6888" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-h "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-h" "127.0.0.1" "${PARAMETERS[@]}") ;;
esac

JAVA_OPTS="-Dfile.encoding=UTF-8"
exec "$JAVA" $JAVA_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" "${PARAMETERS[@]}"

exit $?