#!/bin/bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#


REMOVE_DIRS=(
	include/apache-zookeeper/data
	include/apache-zookeeper/logs
	sbin/data
	sbin/logs
)

for folder in ${REMOVE_DIRS[@]}; do

	if [ -d "$folder" ]; then
		rm -rf $folder
	fi

done

REMOVE_FIELS=(
	iginx.out
	gc.log
	sbin/nohup.out
	include/apache-zookeeper/nohup.out
)

for file in ${REMOVE_FIELS[@]}; do

	if [ -f "$file" ]; then
		rm -f $file
	fi

done

BUILT_IN_UDF=(
  udf_list
  python_scripts/class_loader.py
  python_scripts/constant.py
  python_scripts/py_worker.py
)

mv udf_funcs udf_funcs_bak
mkdir -p udf_funcs/python_scripts
for file in ${BUILT_IN_UDF[@]}; do
  mv udf_funcs_bak/$file udf_funcs/$file
done
rm -rf udf_funcs_bak

echo Done!

