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
package cn.edu.tsinghua.iginx.client.utils;

public class SqlEscapeUtils {

  /**
   * Unescapes backslash sequences inside string literals of a SQL statement. Used by the CLI before
   * calling session so that user-typed {@code \n} becomes newline and {@code \\n} becomes literal
   * \n; the backend uses raw parsing and receives the same bytes as from Java session.
   *
   * @param sql the SQL statement (e.g. user input from CLI)
   * @return the SQL with string literal contents unescaped
   */
  public static String unescapeStringLiteralsInSql(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    StringBuilder result = new StringBuilder(sql.length());
    boolean inString = false;
    char quoteChar = 0;
    StringBuilder literalContent = new StringBuilder();
    int backslashCount = 0;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);

      if (!inString) {
        result.append(c);
        if (c == '\'' || c == '"' || c == '`') {
          inString = true;
          quoteChar = c;
          literalContent.setLength(0);
          backslashCount = 0;
        }
      } else {
        if (c == '\\') {
          literalContent.append(c);
          ++backslashCount;
        } else if (c == quoteChar && (backslashCount & 1) == 0) {
          // End of string (backslash escape only, no double-quote escape)
          result.append(unescapeLiteralContent(literalContent.toString()));
          result.append(c);
          inString = false;
        } else {
          literalContent.append(c);
          backslashCount = 0;
        }
      }
    }
    if (inString) {
      // 建议抛出明确的异常，最好能提示是哪种引号未闭合
      throw new IllegalArgumentException(
          "SQL syntax error: Unclosed string literal. Expected closing quote: " + quoteChar);
    }
    return result.toString();
  }

  private static String unescapeLiteralContent(String content) {
    if (content == null || content.isEmpty()) {
      return content;
    }
    StringBuilder sb = new StringBuilder(content.length());
    int i = 0;
    while (i < content.length()) {
      char c = content.charAt(i);
      if (c == '\\' && i + 1 < content.length()) {
        char next = content.charAt(i + 1);
        switch (next) {
          case 'n':
            sb.append('\n');
            i += 2;
            break;
          case 'r':
            sb.append('\r');
            i += 2;
            break;
          case 't':
            sb.append('\t');
            i += 2;
            break;
          case 'b':
            sb.append('\b');
            i += 2;
            break;
          case 'f':
            sb.append('\f');
            i += 2;
            break;
          case '\\':
            sb.append('\\');
            i += 2;
            break;
          case 'u':
            if (i + 5 < content.length()) {
              try {
                int codePoint = Integer.parseInt(content.substring(i + 2, i + 6), 16);
                sb.append((char) codePoint);
                i += 6;
              } catch (NumberFormatException e) {
                sb.append('\\').append('u');
                i += 2;
              }
            } else {
              sb.append('\\').append('u');
              i += 2;
            }
            break;
          case '\'':
          case '"':
          case '`':
            // Keep \' \" \` as-is so server receives backslash-quote
            sb.append('\\').append(next);
            i += 2;
            break;
          default:
            sb.append(next);
            i += 2;
        }
      } else {
        sb.append(c);
        i++;
      }
    }
    return sb.toString();
  }
}
