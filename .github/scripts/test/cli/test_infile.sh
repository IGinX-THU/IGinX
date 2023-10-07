#!/bin/sh

set -e

sh -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

sh -c "echo 'LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, a, b, c, d)' | xargs -0 -t -i sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

sh -c "rm -rf test/src/test/resources/fileReadAndWrite/byteStream/*"

sh -c "rm -rf test/src/test/resources/fileReadAndWrite/csv/*"
