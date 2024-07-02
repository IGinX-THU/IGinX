#!/bin/bash

REMOVE_DIRS=(
	include/apache-zookeeper/data
	include/apache-zookeeper/logs
	sbin/parquetData
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

