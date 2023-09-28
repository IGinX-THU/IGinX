#!/bin/sh

set -e

sh -c "echo 'LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, a, b, c)' | xargs -0 -t -i sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"
