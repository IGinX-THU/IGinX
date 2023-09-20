#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
  if (( ${half_system_memory_in_mb} > 1024 )); then
    half_system_memory_in_mb=1024
  fi
  if (( ${quarter_system_memory_in_mb} > 65536 )); then
    quarter_system_memory_in_mb=65536
  fi
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

PARAMETERS="$@"

# Added parameters when default parameters are missing

# sh version
case "$PARAMETERS" in
*"-pw "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS="-pw root $PARAMETERS" ;;
esac
case "$PARAMETERS" in
*"-u "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS="-u root $PARAMETERS" ;;
esac
case "$PARAMETERS" in
*"-p "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS="-p 6888 $PARAMETERS" ;;
esac
case "$PARAMETERS" in
*"-h "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS="-h 127.0.0.1 $PARAMETERS" ;;
esac

exec "$JAVA" ${HEAP_OPTS[@]} -cp "$CLASSPATH" "$MAIN_CLASS" $PARAMETERS

exit $?
