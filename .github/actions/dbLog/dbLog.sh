#!/bin/sh

# Only works on WindowsOS
# all logs are stored in {DB_DIR_ROOT}/logs/db.log & {DB_DIR_ROOT}/logs/db-error.log(optional)
dbName=$1

case $dbName in
  IoTDB12)
    dirName=apache-iotdb-0.12.6-server-bin-*
    ;;
  InfluxDB)
    dirName=influxdb2-2.7.4-window*
    ;;
  MongoDB)
    dirName=mongodb-win32-x86_64-windows-6.0.12-*
    ;;
  Redis)
    dirName=Redis-7.0.14-Windows-x64-*
    ;;
  PostgreSQL)
    dirName=pgsql-*
    ;;
  *)
    echo "DB:$dbName log not supported."
    exit 0
    ;;
esac

for dir in $dirName; do
    if [ -d "$dir" ]; then
        echo "Entering: $dir"

        # show db.log & db-error.log
        if [ -f "$dir/logs/db.log" ]; then
            echo "cat $dir/logs/db.log :"
            cat "$dir/logs/db.log"
        else
            echo "$dir/logs/db.log not found."
        fi

        if [ -f "$dir/logs/db-error.log" ]; then
            echo "cat $dir/logs/db-error.log :"
            cat "$dir/logs/db-error.log"
        else
            echo "$dir/logs/db-error.log not found."
        fi
    fi
done

