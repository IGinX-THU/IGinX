#!/bin/sh

set -e

echo "Downloading zip archive. This may take a few minutes..."

sh -c "curl -LJO https://get.enterprisedb.com/postgresql/postgresql-15.5-1-windows-x64-binaries.zip -o postgresql-15.5-1-windows-x64-binaries.zip"

sh -c "unzip -qq postgresql-15.5-1-windows-x64-binaries.zip"

echo "Download finished."

sh -c "ls pgsql/bin"

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#5432#relational#engine=postgresql/storageEngineList=127.0.0.1#5432#relational#engine=postgresql/g" conf/config.properties

for port in "$@"
do

  sh -c "cp -R pgsql pgsql-$port"

  filePrefix="pgsql-$port"

  sh -c "mkdir -p $filePrefix/data"

  sh -c "mkdir -p $filePrefix/logs"

  arguments="-ArgumentList '-D', '$filePrefix/data', '--username=postgres', '--auth', 'trust', '--no-instructions', '-E', 'UTF8'"

  redirect="-RedirectStandardOutput '$filePrefix/logs/db.log' -RedirectStandardError '$filePrefix/logs/db-error.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/initdb' -NoNewWindow $arguments $redirect"

  ctlarguments="-ArgumentList '-D', '$filePrefix/data', '-o', '\"-F -p $port\"', 'start'"

  ctlredirect="-RedirectStandardOutput '$filePrefix/logs/pg_ctl.log' -RedirectStandardError '$filePrefix/logs/pg_ctl-error.log'"

  sh -c "sleep 6"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/pg_ctl' -NoNewWindow $ctlarguments $ctlredirect"

  sql='"ALTER USER postgres WITH PASSWORD '\'\''postgres'\'\'';"'

  sqlarguments="-ArgumentList '-c', $sql"

  sqlredirect="-RedirectStandardOutput '$filePrefix/logs/psql.log' -RedirectStandardError '$filePrefix/logs/psql-error.log'"

  powershell -command "Start-Process -FilePath '$filePrefix/bin/psql' -NoNewWindow $sqlarguments $sqlredirect"
done
