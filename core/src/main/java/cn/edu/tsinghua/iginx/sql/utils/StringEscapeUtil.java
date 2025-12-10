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
package cn.edu.tsinghua.iginx.sql.utils;

public class StringEscapeUtil {

  /** 把字符串中的转义序列（如 \n, \t, \", \', \\ 等）还原为真实字符 */
  public static String unescape(String input) {
    if (input == null) {
      return "";
    }

    StringBuilder target = new StringBuilder(input.length());
    boolean escaping = false;

    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);

      if (!escaping) {
        if (c == '\\') {
          escaping = true;
        } else {
          target.append(c);
        }
        continue;
      }

      // 进入 escaping 状态
      switch (c) {
        case 'b':
          target.append('\b');
          break;
        case 'f':
          target.append('\f');
          break;
        case 'n':
          target.append('\n');
          break;
        case 'r':
          target.append('\r');
          break;
        case 't':
          target.append('\t');
          break;
        case '\\':
          target.append('\\');
          break;
        case '\'':
          target.append('\'');
          break;
        case '\"':
          target.append('\"');
          break;
        case 'u': // Unicode 转义
          if (i + 4 < input.length()) {
            String hex = input.substring(i + 1, i + 5);
            try {
              int code = Integer.parseInt(hex, 16);
              target.append((char) code);
              i += 4;
            } catch (NumberFormatException e) {
              target.append("\\u").append(hex); // 非法的 Unicode 序列，原样输出
              i += 4;
            }
          } else {
            target.append("\\u"); // 不完整的 Unicode 序列
          }
          break;
        default:
          // 非标准转义，原样保留
          target.append('\\').append(c);
      }
      escaping = false;
    }

    // 处理最后一个单独的 '\'
    if (escaping) {
      target.append('\\');
    }

    return target.toString();
  }
}
