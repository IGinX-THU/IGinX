#!/bin/sh
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
 

set -e

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/byteStream"

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/csv"

sh -c "chmod +x client/target/iginx-client-$2/sbin/start_cli.sh"

sh -c "sleep 10"

SCRIPT_COMMAND="xargs -0 -t -I F sh client/target/iginx-client-$2/sbin/start_cli.sh -e 'F'"

sh -c "echo 'clear data;' | ${SCRIPT_COMMAND}"

sh -c "echo 'insert into test(key, s1) values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4);' | ${SCRIPT_COMMAND}"

sh -c "echo 'insert into test(key, s2) values (0, 0.5), (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);' | ${SCRIPT_COMMAND}"

sh -c "echo 'insert into test(key, s3) values (0, true), (1, false), (2, true), (3, false), (4, true);' | ${SCRIPT_COMMAND}"

sh -c "echo 'insert into test(key, s4) values (0, "'"aaa"'"), (1, "'"bbb"'"), (2, "'"ccc"'"), (3, "'"ddd"'"), (4, "'"eee"'");' | ${SCRIPT_COMMAND}"

sh -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/byteStream"'" as stream;' | ${SCRIPT_COMMAND}"

sh -c "echo 'select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" as csv;' | ${SCRIPT_COMMAND}"

mkdir -p "test/src/test/resources/fileReadAndWrite/byteDummy"

# add exported dir as dummy fs storge, then test export
bash -c "cp -r test/src/test/resources/fileReadAndWrite/byteStream/* test/src/test/resources/fileReadAndWrite/byteDummy"
# add extension to filename
for file in test/src/test/resources/fileReadAndWrite/byteDummy/*; do
    mv "$file" "${file}.ext"
done

bash -c "echo 'ADD STORAGEENGINE ("'"127.0.0.1"'", 6670, "'"filesystem"'", "'"dummy_dir=test/src/test/resources/fileReadAndWrite/byteDummy,iginx_port=6888,has_data=true,is_read_only=true"'");show columns byteDummy.*;' | ${SCRIPT_COMMAND}"

bash -c "echo 'select * from byteDummy into outfile "'"test/src/test/resources/fileReadAndWrite/byteStreamExport"'" as stream;' | ${SCRIPT_COMMAND}"

db_name=$1

if [[ "$db_name" != "FileSystem" ]]; then
  exit 0
fi

sh -c "mkdir downloads"

downloadURL="https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/iginx-python-example/largeImg/large_img.jpg"

sh -c "wget -nv $downloadURL --directory-prefix=downloads"

# 将 downloads/large_img.jpg 的数据加载到IGinX数据库中
sh -c "mvn test -q -Dtest=FileLoaderTest#loadLargeImage -DfailIfNoTests=false -P-format"

OUTFILE_COMMAND='select large_img_jpg from downloads into outfile "'"test/src/test/resources/fileReadAndWrite/img_outfile"'" as stream;'

sh -c "echo '$OUTFILE_COMMAND' | xargs -0 -t -I F sh client/target/iginx-client-$2/sbin/start_cli.sh -e 'F'"
