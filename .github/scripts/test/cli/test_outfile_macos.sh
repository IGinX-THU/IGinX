#!/bin/sh

set -e

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/byteStream"

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/csv"

sh -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

sh -c "sleep 10"

sh -c "echo 'clear data;' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "echo 'insert into test(key, s1) values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4);' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "echo 'insert into test(key, s2) values (0, 0.5), (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "echo 'insert into test(key, s3) values (0, true), (1, false), (2, true), (3, false), (4, true);' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "echo 'insert into test(key, s4) values (0, "'"aaa"'"), (1, "'"bbb"'"), (2, "'"ccc"'"), (3, "'"ddd"'"), (4, "'"eee"'");' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/byteStream"'" as stream;' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

sh -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" as csv;' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"

db_name=$1

# 只测FileSystem和Parquet
if [[ "$db_name" != "FileSystem" ]] && [[ "$db_name" != "Parquet" ]]; then
  exit 0
fi

sh -c "mkdir downloads"

downloadURL="https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/iginx-python-example/largeImg/large_img.jpg"

sh -c "wget -nv $downloadURL --directory-prefix=downloads"

# 将 downloads/large_img.jpg 的数据加载到IGinX数据库中
sh -c "mvn test -q -Dtest=FileLoaderTest#loadLargeImage -DfailIfNoTests=false -P-format"

OUTFILE_COMMAND='select large_img_jpg from downloads into outfile "'"test/src/test/resources/fileReadAndWrite/img_outfile"'" as stream;'

sh -c "echo '$OUTFILE_COMMAND' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"
