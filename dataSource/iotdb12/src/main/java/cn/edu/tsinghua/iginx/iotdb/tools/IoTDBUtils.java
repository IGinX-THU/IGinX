/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.iotdb.tools;

public class IoTDBUtils {

  /**
   * 转义 IoTDB 字符串字面量 (用于 WHERE path = 'value') IoTDB 中字符串使用单引号或双引号包裹，内部单引号需转义为 \' 或 '' 这里统一处理为：将 \
   * 替换为 \\，将 ' 替换为 \'
   */
  public static String escapeStringLiteral(String value) {
    if (value == null) {
      return "";
    }
    // 注意替换顺序：先替换反斜杠，再替换单引号
    return value.replace("\\", "\\\\").replace("'", "\\'");
  }

  /**
   * 格式化路径：将路径按点分割，对非通配符的节点加上双引号保护 例如： input: ,"'.nt.status output: " ,\"' "."nt"."status" * input:
   * root.unit*.status output: "root".unit*."status" (保留通配符)
   */
  public static String formatPath(String path) {
    if (path == null || path.isEmpty()) {
      return "";
    }
    // 简单按点分割（假设节点名称内部不包含点，或者点是层级分隔符）
    String[] nodes = path.split("\\.");
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < nodes.length; i++) {
      if (i > 0) {
        sb.append(".");
      }
      String node = nodes[i];

      // 核心逻辑：判断是否需要引用
      if (isWildcard(node)) {
        // 如果是通配符，原样保留，不加引号
        sb.append(node);
      } else {
        // 普通节点，加双引号保护，防止特殊字符破坏 SQL 结构
        sb.append(quoteNode(node));
      }
    }
    return sb.toString();
  }

  /** 判断节点是否包含通配符 IoTDB 的通配符通常是 * 或 **，或者 prefix* */
  private static boolean isWildcard(String node) {
    // 如果节点包含 *，通常表示模糊匹配（如 unit*），也不应该加引号变成字面量
    return node.contains("*");
  }

  /** 为节点名称添加双引号，并转义内部的双引号 */
  public static String quoteNode(String node) {
    if (node == null) return "";
    // 简单策略：如果包含非字母数字下划线，就用双引号包裹
    return "\"" + node.replace("\"", "\\\"") + "\"";
  }
}
