#!/bin/bash

set -e

COMMAND='LOAD DATA FROM INFILE "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" AS CSV INTO t(key, a, b, c, d);'

if [ -n "$MSYSTEM" ]; then

	bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND'"

else

	SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

	bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

	bash -c "echo '$COMMAND' | xargs -0 -t -i ${SCRIPT_COMMAND}"

fi
