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
package cn.edu.tsinghua.iginx.relational.tools;

import cn.edu.tsinghua.iginx.utils.QuotedStringUtils;

/**
 * SQL string utilities for the relational data source. Used when building SQL that is sent to
 * MySQL, PostgreSQL, Oracle, Dameng, etc.
 */
public final class SqlStringUtils {

  private SqlStringUtils() {}

  /**
   * Escapes a string for use inside SQL quoted content (identifier or literal). The quote character
   * is escaped by doubling: " → "", ` → ``, ' → ''. Use when wrapping any string in a given quote
   * character so that the quote inside the content does not break the SQL (e.g. table name "a\"b"
   * or value 'It''s').
   *
   * @param content the string to escape (may be null)
   * @param quoteChar the quote character used (e.g. '"', '`', '\'')
   * @return escaped string, or null if content is null
   */
  public static String escapeSqlQuotedContent(String content, char quoteChar) {
    return QuotedStringUtils.escapeQuotedContent(content, quoteChar);
  }

  /**
   * Wraps a name in the given quote character with content escaped (for identifiers: table, column,
   * schema). Equivalent to quoteChar + escapeSqlQuotedContent(name, quoteChar) + quoteChar.
   *
   * @param name the identifier name (may be null)
   * @param quoteChar the quote character (e.g. '"', '`')
   * @return quoted and escaped string, or "" if name is null
   */
  public static String wrapWithQuotedContent(String name, char quoteChar) {
    return QuotedStringUtils.wrapWithQuotedContent(name, quoteChar);
  }

  /**
   * Escapes a string for use inside a single-quoted SQL string literal ('...'). Use when building
   * VALUES or WHERE literals that are sent to the DB.
   *
   * <p>MySQL treats backslash as escape in single-quoted strings (e.g. \' → '), so a value
   * containing literal backslash+quote (e.g. It\'s ok) must have backslash escaped first (\ → \\)
   * and then quote doubled (' → ''), producing 'It\\''s ok'. PostgreSQL (standard) only uses '' for
   * quote; backslash is literal, so only doubling the quote is correct there.
   *
   * @param content the string to escape (may be null)
   * @param backslashEscaping true for MySQL (and similar); false for PostgreSQL / standard
   * @return escaped string, or null if content is null
   */
  public static String escapeSqlSingleQuotedLiteral(String content, boolean backslashEscaping) {
    if (content == null) {
      return null;
    }
    if (backslashEscaping) {
      return content.replace("\\", "\\\\").replace("'", "''");
    }
    return content.replace("'", "''");
  }
}
