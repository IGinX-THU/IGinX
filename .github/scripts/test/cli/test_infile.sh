#!/bin/bash

set -e

COMMAND='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, d, b, c, a);'

SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

bash -c "echo '$COMMAND' | xargs -0 -t -i ${SCRIPT_COMMAND}"

MCOMMAND='ls /fragment'

MSCRIPT_COMMAND="bash zookeeper/bin/zkCli.sh -e '{}'"

bash -c "chmod +x zookeeper/bin/zkCli.sh"

bash -c "echo '$MCOMMAND' | xargs -0 -t -i ${MSCRIPT_COMMAND}"
