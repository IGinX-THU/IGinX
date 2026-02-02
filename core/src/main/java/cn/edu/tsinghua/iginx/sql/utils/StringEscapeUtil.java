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
  /**
   * Returns the raw content of a string literal token without backslash unescaping. Only strips
   * surrounding quotes and applies quote-doubling ('' → ', "" → ", `` → `). Used when the client
   * has already unescaped the SQL before sending; the backend then does not interpret \n, \\, etc.
   *
   * @param tokenText the string literal token text including quotes
   * @return the content with only quote-doubling applied, no backslash unescape
   */
  public static String unescapeStringLiteral(String tokenText) {
    if (tokenText == null || tokenText.length() < 2) {
      return "";
    }
    char q = tokenText.charAt(0);
    char last = tokenText.charAt(tokenText.length() - 1);
    if (q != last || (q != '\'' && q != '"' && q != '`')) {
      return tokenText;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < tokenText.length() - 1; i++) {
      if (tokenText.charAt(i) == q
          && i + 1 < tokenText.length() - 1
          && tokenText.charAt(i + 1) == q) {
        sb.append(q);
        i++;
      } else {
        sb.append(tokenText.charAt(i));
      }
    }
    return sb.toString();
  }
}
