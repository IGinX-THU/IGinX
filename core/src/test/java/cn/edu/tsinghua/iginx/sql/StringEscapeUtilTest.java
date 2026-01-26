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
package cn.edu.tsinghua.iginx.sql;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.sql.utils.StringEscapeUtil;
import org.junit.Test;

public class StringEscapeUtilTest {

  @Test
  public void testInternalUnescape() {
    // 测试内部 unescape() 方法
    // 常见转义
    assertEquals("hello\nworld", StringEscapeUtil.unescape("hello\\nworld"));
    assertEquals("tab\tindent", StringEscapeUtil.unescape("tab\\tindent"));
    assertEquals("carriage\rreturn", StringEscapeUtil.unescape("carriage\\rreturn"));
    assertEquals("back\bspace", StringEscapeUtil.unescape("back\\bspace"));
    assertEquals("formfeed\fpage", StringEscapeUtil.unescape("formfeed\\fpage"));
    assertEquals("It\\works", StringEscapeUtil.unescape("It\\\\works"));
    assertEquals("a\\.txt", StringEscapeUtil.unescape("a\\\\.txt"));
    // \. 是未知转义序列，忽略反斜杠，只保留点
    assertEquals("a.txt", StringEscapeUtil.unescape("a\\.txt"));
    assertEquals("single'quote", StringEscapeUtil.unescape("single\\'quote"));
    assertEquals("double\"quote", StringEscapeUtil.unescape("double\\\"quote"));
    // \Z -> ASCII 26 (Ctrl+Z, 0x1A)
    assertEquals("ctrl" + (char) 0x1A + "z", StringEscapeUtil.unescape("ctrl\\Zz"));

    // 未知转义序列忽略反斜杠，只保留后面的字符
    assertEquals("a", StringEscapeUtil.unescape("\\a"));
    assertEquals("v\n", StringEscapeUtil.unescape("\\v\\n"));

    // unicode转义
    assertEquals("你好", StringEscapeUtil.unescape("\\u4F60\\u597D"));
    assertEquals("A", StringEscapeUtil.unescape("\\u0041"));
    assertEquals("Ω", StringEscapeUtil.unescape("\\u03A9"));

    // 非法 unicode 序列，应该保留原样
    assertEquals("\\uZZZZ", StringEscapeUtil.unescape("\\uZZZZ"));
    assertEquals("\\u123", StringEscapeUtil.unescape("\\u123")); // 不完整

    // 末尾有个单独的 \
    assertEquals("abc\\", StringEscapeUtil.unescape("abc\\"));

    assertEquals("", StringEscapeUtil.unescape(""));
  }

  @Test
  public void testUnescapeStringLiteral() {
    // ========== All strings now support backslash escaping ==========
    // Escape sequences are processed
    assertEquals("value\ntest", StringEscapeUtil.unescapeStringLiteral("'value\\ntest'"));
    assertEquals("tab\there", StringEscapeUtil.unescapeStringLiteral("'tab\\there'"));
    assertEquals("a\\b", StringEscapeUtil.unescapeStringLiteral("'a\\\\b'"));
    assertEquals("quote'here", StringEscapeUtil.unescapeStringLiteral("'quote\\'here'"));

    // Windows paths need double backslashes
    assertEquals(
        "C:\\Users\\test.py", StringEscapeUtil.unescapeStringLiteral("'C:\\\\Users\\\\test.py'"));
    assertEquals(
        "C:\\temp\\file.txt", StringEscapeUtil.unescapeStringLiteral("\"C:\\\\temp\\\\file.txt\""));
    assertEquals(
        "D:\\data\\test\\file.csv",
        StringEscapeUtil.unescapeStringLiteral("'D:\\\\data\\\\test\\\\file.csv'"));

    // Unix/Linux paths work as before
    assertEquals(
        "/home/user/test.py", StringEscapeUtil.unescapeStringLiteral("'/home/user/test.py'"));
    assertEquals("/tmp/file.txt", StringEscapeUtil.unescapeStringLiteral("\"/tmp/file.txt\""));

    // All escape sequences work
    assertEquals("\r\n\t\b\f\0", StringEscapeUtil.unescapeStringLiteral("'\\r\\n\\t\\b\\f\\0'"));
    assertEquals((char) 0x1A + "", StringEscapeUtil.unescapeStringLiteral("'\\Z'"));
    assertEquals("A", StringEscapeUtil.unescapeStringLiteral("'\\u0041'"));

    // Quote escaping still works (both '' and \')
    assertEquals("It's OK", StringEscapeUtil.unescapeStringLiteral("'It''s OK'"));
    assertEquals("It's OK", StringEscapeUtil.unescapeStringLiteral("'It\\'s OK'"));
    assertEquals("Say \"Hi\"", StringEscapeUtil.unescapeStringLiteral("\"Say \"\"Hi\"\"\""));
    assertEquals("Say \"Hi\"", StringEscapeUtil.unescapeStringLiteral("\"Say \\\"Hi\\\"\""));

    // Double-quoted strings
    assertEquals("value\ntest", StringEscapeUtil.unescapeStringLiteral("\"value\\ntest\""));
    assertEquals("tab\t", StringEscapeUtil.unescapeStringLiteral("\"tab\\t\""));

    // ========== Edge Cases ==========
    assertEquals("", StringEscapeUtil.unescapeStringLiteral("''"));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral("\"\""));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral(""));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral(null));
  }

  @Test
  public void testUnescapeBacktickIdentifier() {
    assertEquals(
        "a.Iris\\.parquet", StringEscapeUtil.unescapeBacktickIdentifier("a.Iris\\.parquet"));
    assertEquals("table`name", StringEscapeUtil.unescapeBacktickIdentifier("table``name"));
    assertEquals("table\\n", StringEscapeUtil.unescapeBacktickIdentifier("table\\n"));
    assertEquals("", StringEscapeUtil.unescapeBacktickIdentifier(""));
    assertEquals("", StringEscapeUtil.unescapeBacktickIdentifier(null));
  }
}
