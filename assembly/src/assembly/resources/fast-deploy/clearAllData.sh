#!/bin/bash

array=(
	include/apache-zookeeper-3.7.0-bin/data
	include/apache-zookeeper-3.7.0-bin/logs
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
	include/apache-zookeeper-3.7.0-bin/nohup.out
)

for file in ${arrayf[@]}; do

	if [ -f "$file" ]; then
		rm -f $file
	fi

done

echo Done!

