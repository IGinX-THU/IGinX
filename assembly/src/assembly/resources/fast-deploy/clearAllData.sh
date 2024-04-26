#!/bin/bash

array=(
	include/apache-zookeeper/data
	include/apache-zookeeper/logs
	sbin/parquetData
	sbin/logs
)

for folder in ${array[@]}; do

	if [ -d "$folder" ]; then
		rm -rf $folder
	fi

done

arrayf=(
	iginx.out
	gc.log
	sbin/nohup.out
	include/apache-zookeeper/nohup.out
)

for file in ${arrayf[@]}; do

	if [ -f "$file" ]; then
		rm -f $file
	fi

done

echo Done!

