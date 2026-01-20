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

/**
 * SQL string literal escape utility, following MySQL/SQL standard escape rules. This utility class
 * provides methods to unescape SQL string literals according to the SQL standard and MySQL
 * conventions.
 *
 * <p>Supported escape sequences:
 *
 * <ul>
 *   <li>Single quote: {@code ''} or {@code \'} → {@code '}
 *   <li>Double quote: {@code ""} or {@code \"} → {@code "}
 *   <li>Backslash: {@code \\} → {@code \}
 *   <li>Newline: {@code \n} → line feed (0x0A)
 *   <li>Carriage return: {@code \r} → carriage return (0x0D)
 *   <li>Tab: {@code \t} → horizontal tab (0x09)
 *   <li>Backspace: {@code \b} → backspace (0x08)
 *   <li>Form feed: {@code \f} → form feed (0x0C)
 *   <li>Null: {@code \0} → null character (0x00)
 *   <li>Unicode: {@code \\uXXXX} → Unicode character (hexadecimal)
 * </ul>
 *
 * <p>This implementation follows MySQL's string literal handling, which is compatible with the SQL
 * standard.
 */
public class StringEscapeUtil {

  /**
   * Unescapes a SQL string literal according to MySQL/SQL standard rules. This method handles all
   * standard SQL escape sequences including single quote escaping ({@code ''} or {@code \'}),
   * backslash sequences, control characters, and Unicode escapes.
   *
   * <p>The input should be the content between the quotes (not including the quotes themselves).
   *
   * @param input the string literal content to unescape (without surrounding quotes)
   * @return the unescaped string
   */
  public static String unescape(String input) {
    if (input == null || input.isEmpty()) {
      return input == null ? "" : input;
    }

    StringBuilder result = new StringBuilder(input.length());
    int length = input.length();
    int i = 0;

    while (i < length) {
      char c = input.charAt(i);

      // Handle backslash escape sequences (MySQL/SQL standard)
      if (c == '\\' && i + 1 < length) {
        char next = input.charAt(i + 1);
        switch (next) {
          case 'n':
            result.append('\n');
            i += 2;
            continue;
          case 'r':
            result.append('\r');
            i += 2;
            continue;
          case 't':
            result.append('\t');
            i += 2;
            continue;
          case 'b':
            result.append('\b');
            i += 2;
            continue;
          case 'f':
            result.append('\f');
            i += 2;
            continue;
          case '0':
            result.append('\0');
            i += 2;
            continue;
          case '\\':
            result.append('\\');
            i += 2;
            continue;
          case '\'':
            result.append('\'');
            i += 2;
            continue;
          case '"':
            result.append('"');
            i += 2;
            continue;
          case 'u':
            // Unicode escape: \\uXXXX
            if (i + 5 < length) {
              try {
                String hex = input.substring(i + 2, i + 6);
                int codePoint = Integer.parseInt(hex, 16);
                result.append((char) codePoint);
                i += 6;
                continue;
              } catch (NumberFormatException e) {
                // Invalid Unicode escape, treat as literal \\u
                result.append('\\').append('u');
                i += 2;
                continue;
              }
            } else {
              // Incomplete Unicode escape
              result.append('\\').append('u');
              i += 2;
              continue;
            }
          default:
            // Unknown escape sequence: MySQL ignores the backslash and keeps only the following
            // character (default behavior, unless NO_BACKSLASH_ESCAPES mode is enabled)
            // For example: \A -> A, \x -> x
            result.append(next);
            i += 2;
            continue;
        }
      }

      // Handle single quote escaping for single-quoted strings: '' -> '
      // Note: This is only valid in single-quoted strings, but we handle it here
      // for compatibility with both single and double-quoted strings
      if (c == '\'' && i + 1 < length && input.charAt(i + 1) == '\'') {
        result.append('\'');
        i += 2;
        continue;
      }

      // Handle double quote escaping for double-quoted strings: "" -> "
      if (c == '"' && i + 1 < length && input.charAt(i + 1) == '"') {
        result.append('"');
        i += 2;
        continue;
      }

      // Regular character
      result.append(c);
      i++;
    }

    return result.toString();
  }

  /**
   * Unescapes a SQL string literal from a token text (including surrounding quotes). This method
   * follows PostgreSQL's E-string syntax:
   *
   * <ul>
   *   <li><b>Standard strings</b> ({@code '...'} or {@code "..."}): Only process quote escaping
   *       ({@code ''} and {@code ""}), backslashes are preserved as-is. This is safe for file paths
   *       like {@code 'C:\Users\test.py'}.
   *   <li><b>Escape strings</b> ({@code E'...'} or {@code E"..."}): Process all escape sequences
   *       including {@code \n}, {@code \t}, {@code \\}, etc. Use this for data values that need
   *       control characters.
   * </ul>
   *
   * <p>Examples:
   *
   * <pre>
   *   'C:\Users\test.py'    → C:\Users\test.py (backslashes preserved)
   *   E'value\ntest'        → value<newline>test (escape processed)
   *   E'C:\\Users\\test.py' → C:\Users\test.py (double backslash becomes single)
   * </pre>
   *
   * @param tokenText the string literal token text including quotes and optional E prefix
   * @return the unescaped string value
   */
  public static String unescapeStringLiteral(String tokenText) {
    if (tokenText == null || tokenText.length() < 2) {
      return "";
    }

    // Check for E-string prefix (PostgreSQL style: E'...' or E"...")
    boolean isEscapeString = false;
    int startIndex = 0;
    if (tokenText.length() >= 3 && (tokenText.charAt(0) == 'E' || tokenText.charAt(0) == 'e')) {
      char secondChar = tokenText.charAt(1);
      if (secondChar == '\'' || secondChar == '"') {
        isEscapeString = true;
        startIndex = 1; // Skip the 'E' prefix
      }
    }

    // Determine quote type and extract content
    if (startIndex < tokenText.length()) {
      char firstQuote = tokenText.charAt(startIndex);
      char lastChar = tokenText.charAt(tokenText.length() - 1);

      if ((firstQuote == '\'' && lastChar == '\'') || (firstQuote == '"' && lastChar == '"')) {
        String content = tokenText.substring(startIndex + 1, tokenText.length() - 1);

        if (isEscapeString) {
          // E-string: process all escape sequences (backslash escapes enabled)
          return unescape(content);
        } else {
          // Standard string: only process quote escaping, preserve backslashes
          return unescapeQuotesOnly(content);
        }
      }
    }

    // If not properly quoted, return as-is (shouldn't happen in valid SQL)
    return tokenText;
  }

  /**
   * Unescapes only quote characters ({@code ''} and {@code ""}), preserving all other characters
   * including backslashes. This is used for standard SQL strings where backslash escape sequences
   * are not processed.
   *
   * @param content the string content (without surrounding quotes)
   * @return the string with only quote escaping processed
   */
  public static String unescapeQuotesOnly(String content) {
    if (content == null || content.isEmpty()) {
      return content == null ? "" : content;
    }

    StringBuilder result = new StringBuilder(content.length());
    int length = content.length();

    for (int i = 0; i < length; i++) {
      char c = content.charAt(i);

      // Handle single quote escaping: '' -> '
      if (c == '\'' && i + 1 < length && content.charAt(i + 1) == '\'') {
        result.append('\'');
        i++; // Skip next quote
        continue;
      }

      // Handle double quote escaping: "" -> "
      if (c == '"' && i + 1 < length && content.charAt(i + 1) == '"') {
        result.append('"');
        i++; // Skip next quote
        continue;
      }

      // All other characters (including backslashes) are preserved as-is
      result.append(c);
    }

    return result.toString();
  }
}
