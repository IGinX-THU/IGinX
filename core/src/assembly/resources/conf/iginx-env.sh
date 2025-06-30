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


#export JAVA_HOME=

###############################################################################
# JVM Configuration
###############################################################################

export IGINX_JAVA_OPTS=()

######################################
# Locale
######################################

# Set the JVM timezone to GMT+8
IGINX_JAVA_OPTS+=(-Duser.timezone=GMT+8)

# Set the file encoding to UTF-8
IGINX_JAVA_OPTS+=(-Dfile.encoding=UTF-8)

######################################
# Memory
######################################

# Followings memory options have auto-detected values by default

# Set the maximum heap size (uncomment and adjust the value as needed)
#IGINX_JAVA_OPTS+=(-Xmx500G) # max heap size

# Set the minimum heap size (uncomment and adjust the value as needed)
#IGINX_JAVA_OPTS+=(-Xms500G) # min heap size

# Set the maximum direct memory size (uncomment and adjust the value as needed)
#IGINX_JAVA_OPTS+=(-XX:MaxDirectMemorySize=300G) # max direct memory size

######################################
# JMX
######################################

# Enable JMX for monitoring, management (uncomment if needed)
#IGINX_JAVA_OPTS+=(-Dcom.sun.management.jmxremote)
#IGINX_JAVA_OPTS+=(-Dcom.sun.management.jmxremote.port=9010)
#IGINX_JAVA_OPTS+=(-Dcom.sun.management.jmxremote.rmi.port=9010)
#IGINX_JAVA_OPTS+=(-Dcom.sun.management.jmxremote.ssl=false)

######################################
# Debugging
######################################

# Uncomment the following line to enable remote debugging
#IGINX_JAVA_OPTS+=(-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005)

######################################
# System Properties
######################################

##################
# Netty
##################

# Disable thread-local caches to avoid memory leaks if number of threads is
# large and the threads are short-lived
IGINX_JAVA_OPTS+=(-Dio.netty.allocator.useCacheForAllThreads=false)

# Set the memory chunk size to 16MB (2^11) rather than the default 4MB (2^9)
# when the page size is 8KB. This can improve allocation performance but will
# increase memory usage
IGINX_JAVA_OPTS+=(-Dio.netty.allocator.maxOrder=11)

##################
# Arrow
##################

# Disable null checks to improve performance
IGINX_JAVA_OPTS+=(-Darrow.enable_null_check_for_get=false)

# Enable unsafe memory access to improve performance (uncomment if needed)
#IGINX_JAVA_OPTS+=(-Darrow.enable_unsafe_memory_access=true)