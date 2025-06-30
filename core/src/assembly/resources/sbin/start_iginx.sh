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

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

if [[ -z "$IGINX_HOME" ]]; then
  export IGINX_HOME=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)
  # ${BASH_SOURCE[0]} instead of $0: $0 will be "bash" if the script is sourced (source / .)
  # Double quoted ${BASH_SOURCE[0]} and $(dirname #):
  # # Avoid Word Splitting and Filename Expansion, 
  # # deal with path contains IFSs and/or *
  # Double dash "--": The end of command options, deal with path starts with "-"
  # && instead of ; or newline: exit code needs to be checked
  
  # Even if IFS contains digit, $? is safe,
  # since there is no Word Splitting in ((expression))
  if (($? != 0)); then
    echo "Cannot determine IGINX_HOME, exit..."
    exit 1
  fi
fi

if [[ -z "$IGINX_CONF_DIR" ]]; then
  IGINX_CONF_DIR="${IGINX_HOME}/conf"
fi

IGINX_CONF="${IGINX_CONF_DIR}/config.properties"
IGINX_DRIVER="${IGINX_HOME}/driver"
IGINX_ENV="${IGINX_CONF_DIR}/iginx-env.sh"
if [[ -f "${IGINX_ENV}" ]]; then
  source "${IGINX_ENV}"
fi

MAIN_CLASS=cn.edu.tsinghua.iginx.Iginx

CLASSPATH="$IGINX_HOME/conf"
for f in "$IGINX_HOME"/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:${f}
done

if [ -n "$JAVA_HOME" ]; then
  for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
    if [ -x "$java" ]; then
      JAVA=$java
      break
    fi
  done
else
  JAVA=java
fi

#computing the memory size for the JVM options
calculate_heap_sizes() {
  case "$(uname)" in
  Linux)
    system_memory_in_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    system_memory_in_mb=$((${system_memory_in_kb} / 1024))
    system_cpu_cores=$(egrep -c 'processor([[:space:]]+):.*' /proc/cpuinfo)
    ;;
  FreeBSD)
    system_memory_in_bytes=$(sysctl hw.physmem | awk '{print $2}')
    system_memory_in_mb=$((${system_memory_in_bytes} / 1024 / 1024))
    system_cpu_cores=$(sysctl hw.ncpu | awk '{print $2}')
    ;;
  SunOS)
    system_memory_in_mb=$(prtconf | awk '/Memory size:/ {print $3}')
    system_cpu_cores=$(psrinfo | wc -l)
    ;;
  Darwin)
    system_memory_in_bytes=$(sysctl hw.memsize | awk '{print $2}')
    system_memory_in_mb=$((${system_memory_in_bytes} / 1024 / 1024))
    system_cpu_cores=$(sysctl hw.ncpu | awk '{print $2}')
    ;;
  *)
    # assume reasonable defaults for e.g. a modern desktop or
    # cheap server
    system_memory_in_mb=2048
    system_cpu_cores=2
    ;;
  esac

  # some systems like the raspberry pi don't report cores, use at least 1
  if (( ${system_cpu_cores} < 1 )); then
    system_cpu_cores=1
  fi

  # set the required memory percentage according to your needs (< 100 & must be a integer)
  max_percentageNumerator=50
  max_heap_size_in_mb=$((${system_memory_in_mb} * ${max_percentageNumerator} / 100))
  MAX_HEAP_SIZE=${max_heap_size_in_mb}M

  min_percentageNumerator=50
  min_heap_size_in_mb=$((${system_memory_in_mb} * ${min_percentageNumerator} / 100))
  MIN_HEAP_SIZE=${min_heap_size_in_mb}M
}

calculate_heap_sizes
# If we use string joined by <space> instead of array,
# we need #unset IFS# or #IFS=' '#
# to ensure Word Splitting works as expected,
# i.e. split $HEAP_OPTS into two arguments
HEAP_OPTS[0]=-Xmx$MAX_HEAP_SIZE
HEAP_OPTS[1]=-Xms$MIN_HEAP_SIZE

# continue to other parameters
LOCAL_JAVA_OPTS=(
 -ea
 -cp "$CLASSPATH"
 -DIGINX_HOME="$IGINX_HOME"
 -DIGINX_DRIVER="$IGINX_DRIVER"
 -DIGINX_CONF="$IGINX_CONF"
 -Dfile.encoding=UTF-8
)

exec "$JAVA" ${HEAP_OPTS[@]} ${IGINX_JAVA_OPTS[@]} ${LOCAL_JAVA_OPTS[@]} "$MAIN_CLASS" "$@"

# Double quoted to avoid Word Splitting when IFS contains digit
exit "$?"
