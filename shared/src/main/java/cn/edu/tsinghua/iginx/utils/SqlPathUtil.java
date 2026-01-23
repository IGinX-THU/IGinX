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
 * Utility for escaping file paths (or other strings) for use in SQL string literals. Since all
 * string literals support backslash escape sequences, paths containing backslashes (e.g. Windows
 * paths) must be escaped before being embedded in SQL.
 *
 * <p>Example: {@code C:\Users\test} → {@code C:\\Users\\test}, so that when the SQL parser
 * unescapes the literal, the original path is preserved.
 */
public final class SqlPathUtil {

  private SqlPathUtil() {}

  /**
   * Escapes backslashes in a path for use in SQL string literals. Use this when embedding file
   * paths (or any string containing backslashes) inside single- or double-quoted SQL string
   * literals.
   *
   * @param path the path or string to escape (may be null)
   * @return the escaped string, or empty string if input is null
   */
  public static String escapePathForSql(String path) {
    if (path == null) {
      return "";
    }
    return path.replace("\\", "\\\\");
  }
}
