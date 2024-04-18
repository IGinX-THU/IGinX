#!/bin/sh
set -e
if [ "$RUNNER_OS" = "Linux" ]; then
  sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties
  sed -i "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties
  sh -c "sudo sh -c 'echo \"deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main\" > /etc/apt/sources.list.d/pgdg.list'"
  sh -c "wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -"
  sh -c "sudo apt-get update"
  sh -c "sudo apt-get -y install postgresql-15"
  sh -c "sudo rm -rf /var/lib/postgresql/15/main"
  sh -c "sudo mkdir -p /var/lib/postgresql/15/main"
  sh -c "sudo chown -R postgres /var/lib/postgresql/15/main"
  sh -c "sudo chmod -R 777 /var/lib/postgresql/15/main"
  sh -c "sudo chmod -R 777 /usr/lib/postgresql/15"
  sh -c "sudo mkdir -p /usr/lib/postgresql-5432"
  sh -c "sudo chmod -R 777 /usr/lib/postgresql-5432"
  sh -c "sudo cp -R /usr/lib/postgresql/15 /usr/lib/postgresql-5432"
  sh -c "sudo mkdir -p /var/lib/postgresql-5432/15/main"
  sh -c "sudo chown -R postgres /var/lib/postgresql-5432/15/main"
  sh -c "sudo chmod -R 777 /var/lib/postgresql-5432/15/main"
  sh -c "sudo su - postgres -c '/usr/lib/postgresql-5432/15/bin/initdb -D /var/lib/postgresql-5432/15/main --auth trust --no-instructions'"
  sh -c "sudo su - postgres -c '/usr/lib/postgresql-5432/15/bin/pg_ctl -D /var/lib/postgresql-5432/15/main -o \"-F -p 5432\" start'"
  sh -c "sudo su - postgres -c '/usr/lib/postgresql-5432/15/bin/psql -c \"ALTER USER postgres WITH PASSWORD '\''postgres'\'';\"'"
elif [ "$RUNNER_OS" = "Windows" ]; then
  echo "Downloading zip archive. This may take a few minutes..."
  sh -c "curl -LJO https://get.enterprisedb.com/postgresql/postgresql-15.5-1-windows-x64-binaries.zip -o postgresql-15.5-1-windows-x64-binaries.zip"
  sh -c "unzip -qq postgresql-15.5-1-windows-x64-binaries.zip"
  echo "Download finished."
  sh -c "ls pgsql/bin"
  sed -i "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties
  sed -i "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties
  sh -c "cp -R pgsql pgsql-5432"
  filePrefix="pgsql-5432"
  sh -c "mkdir -p $filePrefix/data"
  sh -c "mkdir -p $filePrefix/logs"
  arguments="-ArgumentList '-D', '$filePrefix/data', '--username=postgres', '--auth', 'trust', '--no-instructions', '-E', 'UTF8'"
  redirect="-RedirectStandardOutput '$filePrefix/logs/db.log' -RedirectStandardError '$filePrefix/logs/db-error.log'"
  powershell -command "Start-Process -FilePath '$filePrefix/bin/initdb' -NoNewWindow $arguments $redirect"
  ctlarguments="-ArgumentList '-D', '$filePrefix/data', '-o', '\"-F -p 5432\"', 'start'"
  ctlredirect="-RedirectStandardOutput '$filePrefix/logs/pg_ctl.log' -RedirectStandardError '$filePrefix/logs/pg_ctl-error.log'"
  sh -c "sleep 6"
  powershell -command "Start-Process -FilePath '$filePrefix/bin/pg_ctl' -NoNewWindow $ctlarguments $ctlredirect"
  sql='"ALTER USER postgres WITH PASSWORD '\'\''postgres'\'\'';"'
  sqlarguments="-ArgumentList '-c', $sql"
  sqlredirect="-RedirectStandardOutput '$filePrefix/logs/psql.log' -RedirectStandardError '$filePrefix/logs/psql-error.log'"
  powershell -command "Start-Process -FilePath '$filePrefix/bin/psql' -NoNewWindow $sqlarguments $sqlredirect"
elif [ "$RUNNER_OS" = "macOS" ]; then
  sed -i "" "s/storageEngineList=127.0.0.1#6667#iotdb12/#storageEngineList=127.0.0.1#6667#iotdb12/g" conf/config.properties
  sed -i "" "s/#storageEngineList=127.0.0.1#5432#postgresql/storageEngineList=127.0.0.1#5432#postgresql/g" conf/config.properties
  sh -c "wget --quiet https://get.enterprisedb.com/postgresql/postgresql-15.2-1-osx-binaries.zip"
  sh -c "sudo unzip -q postgresql-15.2-1-osx-binaries.zip"
  sh -c "sudo dscl . -create /Users/postgres UniqueID 666"
  sh -c "sudo dscl . -create /Groups/postgres PrimaryGroupID 777"
  sh -c "sudo dscl . -create /Users/postgres PrimaryGroupID 777"
  sh -c "sudo dscl . -create /Users/postgres UserShell /bin/bash"
  sh -c "sudo dscl . -create /Users/postgres RealName \"PostgreSQL\""
  sh -c "sudo dscl . create /Groups/postgres passwd '*'"
  sh -c "sudo mkdir /Users/postgres"
  sh -c "sudo chown -R postgres:postgres /Users/postgres"
  sh -c "sudo dscl . -create /Users/postgres NFSHomeDirectory /Users/postgres"
  sh -c "sudo mkdir -p /var/lib/postgresql/15/main"
  sh -c "sudo chown -R postgres /var/lib/postgresql/15/main"
  sh -c "sudo chmod -R 777 /var/lib/postgresql/15/main"
  sh -c "sudo mkdir -p /Users/postgres/pgsql-5432"
  sh -c "sudo chmod -R 777 /Users/postgres/pgsql-5432"
  sh -c "sudo cp -R pgsql /Users/postgres/pgsql-5432"
  sh -c "sudo mkdir -p /var/lib/postgresql-5432/15/main"
  sh -c "sudo chown -R postgres /var/lib/postgresql-5432/15/main"
  sh -c "sudo chmod -R 777 /var/lib/postgresql-5432/15/main"
  sh -c "sudo su - postgres -c '/Users/postgres/pgsql-5432/pgsql/bin/initdb -D /var/lib/postgresql-5432/15/main --auth trust --no-instructions'"
  sh -c "sudo su - postgres -c '/Users/postgres/pgsql-5432/pgsql/bin/pg_ctl -D /var/lib/postgresql-5432/15/main -o \"-F -p 5432\" start'"
  sh -c "sudo su - postgres -c '/Users/postgres/pgsql-5432/pgsql/bin/psql -c \"ALTER USER postgres WITH PASSWORD '\''postgres'\'';\"'"
else
  echo "$RUNNER_OS is not supported"
  exit 1
fi