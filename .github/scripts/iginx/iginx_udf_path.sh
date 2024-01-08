#!/bin/sh

set -e

cd core/target/iginx-core-0.6.0-SNAPSHOT/

iginx_home_path=$PWD

cd ..

if [ -n "$MSYSTEM" ]; then
    windows_path=$(cygpath -w "$iginx_home_path")

    export IGINX_HOME=$windows_path

    powershell -Command "Start-Process -FilePath 'iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.bat' -NoNewWindow -RedirectStandardOutput '../../iginx-udf.log' -RedirectStandardError '../../iginx-udf-error.log'"
else
    export IGINX_HOME=$iginx_home_path

    sh -c "chmod +x iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh"

    sh -c "nohup iginx-core-0.6.0-SNAPSHOT/sbin/start_iginx.sh > ../../iginx.log 2>&1 &"
fi
