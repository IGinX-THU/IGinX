#!/bin/sh

set -e

sh -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

sh -c "echo 'LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, d, b, c, a);' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "cat 'test/src/test/resources/fileReadAndWrite/csv/test.csv' > 'test/src/test/resources/fileReadAndWrite/csv/test1'"

sh -c "sed -i '1ikey,d m,b,[c],a' 'test/src/test/resources/fileReadAndWrite/csv/test1'"

sh -c "echo 'LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test1"'" AS CSV INTO t1;' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"
