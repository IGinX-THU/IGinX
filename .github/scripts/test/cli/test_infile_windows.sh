#!/bin/bash

set -e

COMMAND='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, d, b, c, a);'

bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND'"

echo "key,d m,b,[c],a" > "test/src/test/resources/fileReadAndWrite/csv/test1"

cat "test/src/test/resources/fileReadAndWrite/csv/test.csv" >> "test/src/test/resources/fileReadAndWrite/csv/test1"

COMMAND1='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test1"'" AS CSV INTO t1;'

bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND1'"
