#!/bin/bash
SCRIPT_COMMAND="/home/lhz/lhz/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"
BATCH=10
ALL=10

for ((k = 1; k <= 19; k++)); do
    P="a$k"
    for ((i = 0; i <= $ALL-$BATCH; i += $BATCH)); do
        COMMAND=""
        for ((j = i; j < i + $BATCH && j <= 100000; j++)); do
            value=$((j + 1))
            COMMAND+="($j, $value),"
        done

        COMMAND=${COMMAND%,}

        COMMAND="insert into a.b(key, $P) values $COMMAND;"
        # 创建临时文件
        TEMP_FILE=./tmp/$P_$i.txt

        echo "$COMMAND" > "$TEMP_FILE"
        # 使用重定向输入的方式执行命令
        bash -c "cat $TEMP_FILE | $SCRIPT_COMMAND > output.txt 2>&1"
#        bash -c "$SCRIPT_COMMAND > output.txt 2>&1"
    done
done
echo "insert done"
