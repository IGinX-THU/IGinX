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
  public void testUnescapeStringLiteral() {
    // Raw parsing: only quote stripping and doubled-quote collapse, no backslash unescape
    assertEquals("value\\ntest", StringEscapeUtil.unescapeStringLiteral("'value\\ntest'"));
    assertEquals("tab\\there", StringEscapeUtil.unescapeStringLiteral("'tab\\there'"));
    assertEquals("a\\\\b", StringEscapeUtil.unescapeStringLiteral("'a\\\\b'"));

    assertEquals(
        "C:\\\\Users\\\\test.py",
        StringEscapeUtil.unescapeStringLiteral("'C:\\\\Users\\\\test.py'"));
    assertEquals(
        "C:\\\\temp\\\\file.txt",
        StringEscapeUtil.unescapeStringLiteral("\"C:\\\\temp\\\\file.txt\""));
    assertEquals(
        "D:\\\\data\\\\test\\\\file.csv",
        StringEscapeUtil.unescapeStringLiteral("'D:\\\\data\\\\test\\\\file.csv'"));

    assertEquals(
        "/home/user/test.py", StringEscapeUtil.unescapeStringLiteral("'/home/user/test.py'"));
    assertEquals("/tmp/file.txt", StringEscapeUtil.unescapeStringLiteral("\"/tmp/file.txt\""));

    // Quote doubling only: '' → ', "" → ", `` → `
    assertEquals("It's OK", StringEscapeUtil.unescapeStringLiteral("'It''s OK'"));
    assertEquals("Say \"Hi\"", StringEscapeUtil.unescapeStringLiteral("\"Say \"\"Hi\"\"\""));
    assertEquals("foo`bar", StringEscapeUtil.unescapeStringLiteral("`foo``bar`"));

    // Edge cases
    assertEquals("", StringEscapeUtil.unescapeStringLiteral("''"));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral("\"\""));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral("``"));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral(""));
    assertEquals("", StringEscapeUtil.unescapeStringLiteral(null));
  }
}
