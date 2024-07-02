#!/usr/bin/env bash
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
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

  # set max heap size based on the following
  # max(min(1/2 ram, 1024MB), min(1/4 ram, 64GB))
  # calculate 1/2 ram and cap to 1024MB
  # calculate 1/4 ram and cap to 65536MB
  # pick the max
  half_system_memory_in_mb=$((${system_memory_in_mb} / 2))
  quarter_system_memory_in_mb=$((${half_system_memory_in_mb} / 2))
#  if (( ${half_system_memory_in_mb} > 1024 )); then
#    half_system_memory_in_mb=1024
#  fi
#  if (( ${quarter_system_memory_in_mb} > 65536 )); then
#    quarter_system_memory_in_mb=65536
#  fi
  if (( ${half_system_memory_in_mb} > ${quarter_system_memory_in_mb} )); then
    max_heap_size_in_mb=$half_system_memory_in_mb
  else
    max_heap_size_in_mb=$quarter_system_memory_in_mb
  fi
  MAX_HEAP_SIZE=${max_heap_size_in_mb}M
}

calculate_heap_sizes
# If we use string joined by <space> instead of array,
# we need #unset IFS# or #IFS=' '#
# to ensure Word Splitting works as expected,
# i.e. split $HEAP_OPTS into two arguments
HEAP_OPTS[0]=-Xmx$MAX_HEAP_SIZE
HEAP_OPTS[1]=-Xms$MAX_HEAP_SIZE

# continue to other parameters
ICONF="$IGINX_HOME/conf/config.properties"
IDRIVER="$IGINX_HOME/driver/"

export IGINX_CONF=$ICONF
export IGINX_DRIVER=$IDRIVER

exec "$JAVA" -Duser.timezone=GMT+8 ${HEAP_OPTS[@]} -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

# Double quoted to avoid Word Splitting when IFS contains digit
exit "$?"
