#!/bin/bash

if [ "$RUNNER_OS" = "Windows" ]; then
  python thu_cloud_download.py \
    -l https://cloud.tsinghua.edu.cn/d/740c158819bc4759a36e/ \
    -s  "."
    # 清除 polybench 目录中所有 .sql 文件中的 ' + 28800000' 字符串（时区问题）
      dir="test/src/test/resources/polybench/queries/"

      # 遍历目录中的所有 .sql 文件
      for file in "$dir"*.sql; do
        # 检查文件是否存在
        if [[ -f "$file" ]]; then
          # 使用 sed 命令查找并替换 ' + 28800000' 字符串
          sed -i 's/ + 28800000//g' "$file"
          echo "Processed $file"
        else
          echo "No .sql files found in the directory."
        fi
      done
else
  python3 thu_cloud_download.py \
    -l https://cloud.tsinghua.edu.cn/d/740c158819bc4759a36e/ \
    -s  "."
fi
  cd tpchdata
  # 目标文件夹路径
  destination_folder="../tpc/TPC-H V3.0.1/data"

  # 确保目标文件夹存在，如果不存在则创建
  mkdir -p "$destination_folder"

  # 将所有*.tbl文件移动到目标文件夹
  mv *.tbl "$destination_folder/"
  cd "$destination_folder"

  chmod +r customer.tbl
  chmod +r lineitem.tbl
  chmod +r nation.tbl
  chmod +r orders.tbl
  chmod +r region.tbl
  chmod +r supplier.tbl
  ls -a
  pwd
  echo "文件移动完成"
