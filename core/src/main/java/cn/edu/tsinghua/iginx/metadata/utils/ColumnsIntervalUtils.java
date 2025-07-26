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
package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import javax.annotation.Nullable;

public class ColumnsIntervalUtils {

  private static final char DELIMITER = '-';

  public static String toString(ColumnsInterval interval) {
    return encode(interval.getStartColumn()) + DELIMITER + encode(interval.getEndColumn());
  }

  public static ColumnsInterval fromString(String str) {
    int pos = str.indexOf(DELIMITER);

    if (pos == -1) {
      throw new IllegalArgumentException("Delimiter not found in string: " + str);
    }

    String startColumn = str.substring(0, pos);
    String endColumn = str.substring(pos + 1);

    return new ColumnsInterval(decode(startColumn), decode(endColumn));
  }

  private static String encode(@Nullable String str) {
    if (str == null) {
      return "\\N"; // 使用特殊标记表示 null
    }
    StringBuilder encoded = new StringBuilder();
    for (char c : str.toCharArray()) {
      switch (c) {
        case DELIMITER:
          encoded.append("\\D"); // 转义 DELIMITER
          break;
        case '\\':
          encoded.append("\\\\"); // 转义反斜杠
          break;
        default:
          encoded.append(c); // 保持其他字符不变
          break;
      }
    }
    return encoded.toString();
  }

  private static String decode(String str) {
    if ("\\N".equals(str)) {
      return null; // 还原 null
    }
    StringBuilder decoded = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (escaped) {
        switch (c) {
          case 'D':
            decoded.append(DELIMITER); // 还原 DELIMITER
            break;
          case '\\':
            decoded.append('\\'); // 还原反斜杠
            break;
          default:
            throw new IllegalArgumentException(
                "Illegal escape sequence at " + i + " of string: " + str);
        }
        escaped = false;
      } else {
        if (c == '\\') {
          escaped = true;
        } else {
          decoded.append(c); // 保持其他字符不变
        }
      }
    }
    if (escaped) {
      throw new IllegalArgumentException("Invalid escape at end of string: " + str);
    }
    return decoded.toString();
  }
}
