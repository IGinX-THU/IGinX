#!/bin/bash

# 指定文件路径
file_path="/home/lhz/lhz/iginx/conn_test/test_shell/read_output_all.txt"  # 替换为你的文件路径

# 统计匹配到的字符串数量和总耗时
count=$(grep -o "Time cost: [0-9]\+ ms" "$file_path" | wc -l)
total_time=$(grep -o "Time cost: [0-9]\+ ms" "$file_path" | awk '{ total += $3 } END { print total }')

# 计算平均耗时
if [ "$count" -eq 0 ]; then
    average_time=0
else
    average_time=$(echo "scale=2; $total_time / $count" | bc)
fi

# 打印结果
echo "总共匹配到 $count 个字符串"
echo "平均耗时为 $average_time ms"
