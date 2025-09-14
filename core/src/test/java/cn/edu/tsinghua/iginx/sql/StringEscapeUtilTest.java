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
  public void testSqlUnescape() {
    // 常见转义
    assertEquals("hello\nworld", StringEscapeUtil.unescape("hello\\nworld"));
    assertEquals("tab\tindent", StringEscapeUtil.unescape("tab\\tindent"));
    assertEquals("carriage\rreturn", StringEscapeUtil.unescape("carriage\\rreturn"));
    assertEquals("back\bspace", StringEscapeUtil.unescape("back\\bspace"));
    assertEquals("formfeed\fpage", StringEscapeUtil.unescape("formfeed\\fpage"));
    assertEquals("It\\works", StringEscapeUtil.unescape("It\\\\works"));
    assertEquals("a\\.txt", StringEscapeUtil.unescape("a\\\\.txt"));
    assertEquals("a\\.txt", StringEscapeUtil.unescape("a\\.txt"));

    // Java 不支持 \a \v 这种转义，原样保留
    assertEquals("\\a", StringEscapeUtil.unescape("\\a"));
    assertEquals("\\v\n", StringEscapeUtil.unescape("\\v\\n"));

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
}
