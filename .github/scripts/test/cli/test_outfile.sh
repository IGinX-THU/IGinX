#!/bin/bash

set -e

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/byteStream"

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/csv"

bash -c "chmod +x client/target/iginx-client-*-SNAPSHOT/sbin/start_cli.sh"

bash -c "sleep 10"

SCRIPT_COMMAND="xargs -0 -t -i bash client/target/iginx-client-*-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "echo 'clear data;' | xargs -t -i bash client/target/iginx-client-*-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "echo 'insert into test(key, s1) values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4);' | ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s2) values (0, 0.5), (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);' | ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s3) values (0, true), (1, false), (2, true), (3, false), (4, true);' | ${SCRIPT_COMMAND}"

bash -c "echo 'insert into test(key, s4) values (0, "'"aaa"'"), (1, "'"bbb"'"), (2, "'"ccc"'"), (3, "'"ddd"'"), (4, "'"eee"'");' | ${SCRIPT_COMMAND}"

bash -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/byteStream"'" as stream;' | ${SCRIPT_COMMAND}"

bash -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" as csv;' | ${SCRIPT_COMMAND}"

mkdir -p "test/src/test/resources/fileReadAndWrite/byteDummy"

# add exported dir as dummy fs storge, then test export
bash -c "cp -r test/src/test/resources/fileReadAndWrite/byteStream/* test/src/test/resources/fileReadAndWrite/byteDummy"
# add extension to filename
for file in test/src/test/resources/fileReadAndWrite/byteDummy/*; do
    mv "$file" "${file}.ext"
done

bash -c "echo 'ADD STORAGEENGINE ("'"127.0.0.1"'", 6670, "'"filesystem"'", "'"dummy_dir:test/src/test/resources/fileReadAndWrite/byteDummy,iginx_port:6888,has_data:true,is_read_only:true"'");show columns byteDummy.*;' | ${SCRIPT_COMMAND}"

bash -c "echo 'select * from byteDummy into outfile "'"test/src/test/resources/fileReadAndWrite/byteStreamExport"'" as stream;' | ${SCRIPT_COMMAND}"

db_name=$1

# 只测FileSystem和Parquet
if [[ "$db_name" != "FileSystem" ]] && [[ "$db_name" != "Parquet" ]]; then
  exit 0
fi

bash -c "mkdir downloads"

downloadURL="https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/iginx-python-example/largeImg/large_img.jpg"

bash -c "wget -nv $downloadURL --directory-prefix=downloads"

# 将 downloads/large_img.jpg 的数据加载到IGinX数据库中
bash -c "mvn test -q -Dtest=FileLoaderTest#loadLargeImage -DfailIfNoTests=false -P-format"

bash -c "echo 'select large_img_jpg from downloads into outfile "'"test/src/test/resources/fileReadAndWrite/img_outfile"'" as stream;' | xargs -0 -t -i bash client/target/iginx-client-*-SNAPSHOT/sbin/start_cli.sh -e '{}'"
