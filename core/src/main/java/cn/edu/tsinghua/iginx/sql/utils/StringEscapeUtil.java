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
   * Returns the unescaped content of a string literal token. Strips surrounding quotes and applies
   * backslash escape for all quote types: \' → ', \" → ", \` → `, \\ → \.
   *
   * @param tokenText the string literal token text including quotes
   * @return the content with backslash unescape applied
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
    String inner = tokenText.substring(1, tokenText.length() - 1);
    // Single-, double-, or backtick-quoted: backslash unescape (\' → ', \" → ", \` → `, \\ → \)
    return unescapeBackslashQuoted(inner, q);
  }

  /** Unescapes backslash sequences: \' → ', \" → ", \` → `, \\ → \. */
  private static String unescapeBackslashQuoted(String content, char quoteChar) {
    StringBuilder sb = new StringBuilder(content.length());
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (c == '\\' && i + 1 < content.length()) {
        char next = content.charAt(i + 1);
        if (next == '\\') {
          sb.append('\\');
        } else if (next == '\'' || next == '"' || next == '`') {
          sb.append(next);
        } else {
          sb.append(c).append(next);
        }
        i++; // skip next char
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
