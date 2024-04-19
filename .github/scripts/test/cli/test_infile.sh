#!/bin/bash

set -e

COMMAND='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, d, b, c, a);'

SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

bash -c "echo '$COMMAND' | xargs -0 -t -i ${SCRIPT_COMMAND}"

bash -c "cat 'test/src/test/resources/fileReadAndWrite/csv/test.csv' > 'test/src/test/resources/fileReadAndWrite/csv/test1'"

bash -c "sed -i '1ikey,d m,b,[c],a' 'test/src/test/resources/fileReadAndWrite/csv/test1'"

COMMAND1='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test1"'" AS CSV INTO t1;'

bash -c "echo '$COMMAND1' | xargs -0 -t -i ${SCRIPT_COMMAND}"
