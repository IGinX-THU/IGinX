#!/bin/bash

# 指定文件路径
file_path="/home/lhz/lhz/iginx/conn_test/test_shell/read_output_all.txt"  # 替换为你的文件路径

# 提取匹配到的耗时值，并将其写入临时文件
grep -o "Time cost: [0-9]\+ ms" "$file_path" | awk '{ print $3 }' > all_time_file.txt