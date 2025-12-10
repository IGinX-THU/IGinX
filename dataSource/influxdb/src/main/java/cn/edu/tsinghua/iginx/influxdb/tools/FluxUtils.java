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
package cn.edu.tsinghua.iginx.influxdb.tools;

/** InfluxDB Flux 查询语句生成的字符串处理工具类 */
public class FluxUtils {

  /** 处理精确匹配的字符串转义 (用于 == "value") 转义双引号和反斜杠 */
  public static String escapeStringLiteral(String value) {
    if (value == null) return "";
    // 替换 \ 为 \\，替换 " 为 \"
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  /**
   * 处理通配符匹配的正则转义 (用于 =~ /value/) 将字符串视为包含通配符 * 的普通文本： 1. 保留 * 作为通配符 (转为 .*) 2. 转义其他所有正则特殊字符（如 . ( )
   * [ ] 等） 3. 转义 Flux 正则定界符 /
   */
  public static String escapeRegexWithWildcard(String value) {
    if (value == null) return "";
    StringBuilder sb = new StringBuilder();
    for (char c : value.toCharArray()) {
      if (c == '*') {
        sb.append(".*"); // 将通配符 * 转换为正则 .*
      } else if (isRegexSpecialChar(c)) {
        sb.append("\\").append(c); // 转义正则特殊字符
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** 判断字符是否为正则表达式特殊字符或 Flux 正则定界符 */
  private static boolean isRegexSpecialChar(char c) {
    // Flux 使用 / 包裹正则，所以 / 也必须转义
    return ".+?^$[](){}|/\\".indexOf(c) != -1;
  }

  /** (可选) 仅转义正则定界符和反斜杠 如果输入的本身就是正则表达式，不应该转义 . 或 *，只转义 / 防止语法错误 */
  public static String escapeRegexRaw(String value) {
    if (value == null) return "";
    return value.replace("\\", "\\\\").replace("/", "\\/");
  }
}
