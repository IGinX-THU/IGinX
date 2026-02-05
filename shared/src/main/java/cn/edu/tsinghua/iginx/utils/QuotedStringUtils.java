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
package cn.edu.tsinghua.iginx.utils;

/**
 * Utilities for escaping and wrapping strings with a quote character. Used when building query
 * strings for SQL, Cypher, and other backends where the quote character inside content is escaped
 * by doubling (e.g. ' → '', " → "", ` → ``).
 */
public final class QuotedStringUtils {

  private QuotedStringUtils() {}

  /**
   * Escapes a string for use inside quoted content. The quote character is escaped by doubling. Use
   * when wrapping any string in a given quote so that the quote inside the content does not break
   * the query (e.g. identifier "a\"b" or literal 'It''s').
   *
   * @param content the string to escape (may be null)
   * @param quoteChar the quote character used (e.g. '"', '`', '\'')
   * @return escaped string, or null if content is null
   */
  public static String escapeQuotedContent(String content, char quoteChar) {
    if (content == null) {
      return null;
    }
    return content.replace(String.valueOf(quoteChar), String.valueOf(quoteChar) + quoteChar);
  }

  /**
   * Wraps a name in the given quote character with content escaped (for identifiers or literals).
   * Equivalent to quoteChar + escapeQuotedContent(name, quoteChar) + quoteChar.
   *
   * @param name the name to wrap (may be null)
   * @param quoteChar the quote character (e.g. '"', '`', '\'')
   * @return quoted and escaped string, or "" if name is null
   */
  public static String wrapWithQuotedContent(String name, char quoteChar) {
    if (name == null) {
      return "";
    }
    return quoteChar + escapeQuotedContent(name, quoteChar) + quoteChar;
  }
}
