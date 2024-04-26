#!/bin/bash

# 读取 tbl 文件
input_file="../IGinX/tpc/TPC-H V3.0.1/data/nation.tbl"
output_file="../IGinX/tpc/TPC-H V3.0.1/data/nation.csv"

# 如果文件已存在，则删除
if [ -f "$output_file" ]; then
    rm "$output_file"
fi

# 处理每一行数据
while IFS='|' read -r fields; do
    # 移除逗号并用双引号括起字符串
    fields=$(echo "$fields" | tr -d ',' | sed 's/^\|$/"/g')
    # 将 "|" 分隔符替换为 ","
    fields=$(echo "$fields" | tr '|' ',')
    # 删除每行末尾的逗号
    fields=$(echo "$fields" | sed 's/,$//')
    # 将处理后的数据写入 CSV 文件
    echo "$fields" >> "$output_file"
done < "$input_file"

echo "Conversion completed. CSV file: $output_file"

# 插入数据

COMMAND1="LOAD DATA FROM INFILE $output_file AS CSV SKIPPING HEADER INTO nation(key, n_name, n_regionkey, n_comment);"
SCRIPT_COMMAND="bash client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e '{}'"

bash -c "chmod +x client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh"

if [ "$RUNNER_OS" = "Linux" ]; then
  bash -c "echo '$COMMAND1' | xargs -0 -t -i ${SCRIPT_COMMAND}"
elif [ "$RUNNER_OS" = "Windows" ]; then
  bash -c "client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.bat -e '$COMMAND1'"
elif [ "$RUNNER_OS" = "macOS" ]; then
  sh -c "echo '$COMMAND1' | xargs -0 -t -I F sh client/target/iginx-client-0.6.0-SNAPSHOT/sbin/start_cli.sh -e 'F'"
fi
