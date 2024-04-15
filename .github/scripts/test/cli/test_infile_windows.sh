#!/bin/bash

set -e

COMMAND='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, d, b, c, a);'

bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND'"

cat "test/src/test/resources/fileReadAndWrite/csv/test.csv" > "test/src/test/resources/fileReadAndWrite/csv/test1"

sed -i "1ikey,d m,b,[c],a" "test/src/test/resources/fileReadAndWrite/csv/test1"

COMMAND1='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test1"'" AS CSV INTO t1 at 10;'

bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND1'"
