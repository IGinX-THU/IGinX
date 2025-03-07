@REM
@REM IGinX - the polystore system with high performance
@REM Copyright (C) Tsinghua University
@REM TSIGinX@gmail.com
@REM
@REM This program is free software; you can redistribute it and/or
@REM modify it under the terms of the GNU Lesser General Public
@REM License as published by the Free Software Foundation; either
@REM version 3 of the License, or (at your option) any later version.
@REM
@REM This program is distributed in the hope that it will be useful,
@REM but WITHOUT ANY WARRANTY; without even the implied warranty of
@REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
@REM Lesser General Public License for more details.
@REM
@REM You should have received a copy of the GNU Lesser General Public License
@REM along with this program; if not, write to the Free Software Foundation,
@REM Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
@REM

@echo off

@REM set JAVA_HOME=

@REM ###########################################################################
@REM # JVM Configuration
@REM ###########################################################################

set IGINX_JAVA_OPTS=

@REM ######################################
@REM # Locale
@REM ######################################

@REM Set the JVM timezone to GMT+8
set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Duser.timezone=GMT+8

@REM Set the file encoding to UTF-8
set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dfile.encoding=UTF-8

@REM ######################################
@REM # Memory
@REM ######################################

@REM Followings memory options have auto-detected values by default

@REM Set the maximum heap size (uncomment and adjust the value as needed)
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Xmx500G

@REM Set the minimum heap size (uncomment and adjust the value as needed)
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Xms500G

@REM Set the maximum direct memory size (uncomment and adjust the value as needed)
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -XX:MaxDirectMemorySize=300G

@REM ######################################
@REM # JMX
@REM ######################################

@REM Enable JMX for monitoring, management (uncomment if needed)
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dcom.sun.management.jmxremote
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dcom.sun.management.jmxremote.port=9010
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dcom.sun.management.jmxremote.rmi.port=9010
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dcom.sun.management.jmxremote.ssl=false

@REM ######################################
@REM # Debugging
@REM ######################################

@REM Uncomment the following line to enable remote debugging
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

@REM ######################################
@REM # System Properties
@REM ######################################

@REM ##################
@REM # Netty
@REM ##################

@REM Disable thread-local caches to avoid memory leaks if number of threads is
@REM large and the threads are short-lived
set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dio.netty.allocator.useCacheForAllThreads=false

@REM Set the memory chunk size to 16MB (2^11) rather than the default 4MB (2^9)
@REM when the page size is 8KB. This can improve allocation performance but will
@REM increase memory usage
set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Dio.netty.allocator.maxOrder=11

@REM ##################
@REM # Arrow
@REM ##################

@REM Disable null checks to improve performance
set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Darrow.enable_null_check_for_get=false

@REM Enable unsafe memory access to improve performance (uncomment if needed)
@REM set IGINX_JAVA_OPTS=%IGINX_JAVA_OPTS% -Darrow.enable_unsafe_memory_access=true