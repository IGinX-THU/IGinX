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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import lombok.Value;

public class LikeOptimizer {

  public enum Kind {
    EXACT,
    STARTS_WITH,
    ENDS_WITH,
    CONTAINS,
    REGEX
  }

  @Value
  public static class Classified {
    Kind kind;
    String literal;
  }

  // 只识别简单、可安全优化的 regex；复杂表达式一律 REGEX
  public static Classified classify(String regex) {
    if (regex == null) return new Classified(Kind.REGEX, null);

    // Pattern.matches 本来就是整串匹配，^$ 可有可无
    String r = stripAnchors(regex);

    if (isPlainLiteral(r)) return new Classified(Kind.EXACT, unescapeLiteral(r));

    if (r.endsWith(".*")) {
      String head = r.substring(0, r.length() - 2);
      if (isPlainLiteral(head)) return new Classified(Kind.STARTS_WITH, unescapeLiteral(head));
    }

    if (r.startsWith(".*")) {
      String tail = r.substring(2);
      if (isPlainLiteral(tail)) return new Classified(Kind.ENDS_WITH, unescapeLiteral(tail));
    }

    if (r.startsWith(".*") && r.endsWith(".*") && r.length() >= 4) {
      String mid = r.substring(2, r.length() - 2);
      if (isPlainLiteral(mid)) return new Classified(Kind.CONTAINS, unescapeLiteral(mid));
    }

    return new Classified(Kind.REGEX, r);
  }

  private static String stripAnchors(String r) {
    if (r.startsWith("^")) r = r.substring(1);
    if (r.endsWith("$")) r = r.substring(0, r.length() - 1);
    return r;
  }

  // 只允许“字面量 + 反斜杠转义的字面量”
  private static boolean isPlainLiteral(String s) {
    boolean escaped = false;
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch == '\\') {
        escaped = true;
        continue;
      }
      if ("[](){}+*?.^$|".indexOf(ch) >= 0) return false;
    }
    return !escaped;
  }

  private static String unescapeLiteral(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    boolean escaped = false;
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (escaped) {
        sb.append(ch);
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }
}
