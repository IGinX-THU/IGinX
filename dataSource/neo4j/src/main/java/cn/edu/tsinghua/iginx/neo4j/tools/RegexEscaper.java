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
package cn.edu.tsinghua.iginx.neo4j.tools;

import java.util.HashSet;
import java.util.Set;

public class RegexEscaper {
  /**
   * 转义字符串中所有正则表达式元字符
   *
   * @param input 原始字符串
   * @return 转义后的字符串（所有正则元字符前加反斜杠 `\`）
   */
  public static String escapeRegex(String input) {
    if (input == null) {
      return null;
    }

    // 正则表达式元字符集合（需要转义的字符）
    Set<Character> regexMetaChars =
        new HashSet<Character>() {
          {
            add('\\'); // 反斜杠本身需要转义
            add('^'); // 行首
            add('$'); // 行尾
            add('.'); // 任意字符
            add('|'); // 或
            add('?'); // 零或一次
            add('*'); // 零或多次
            add('+'); // 一或多次
            add('('); // 分组开始
            add(')'); // 分组结束
            add('['); // 字符类开始
            add(']'); // 字符类结束
            add('{'); // 量词开始
            add('}'); // 量词结束
          }
        };

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (regexMetaChars.contains(c)) {
        sb.append('\\'); // 添加转义反斜杠
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
