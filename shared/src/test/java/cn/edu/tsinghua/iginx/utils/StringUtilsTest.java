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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class StringUtilsTest {

  @Before
  public void setUp() {}

  @Test
  public void reformatPathTest() {
    String str = ".^${}+?()[]|\\\\*";
    String result = StringUtils.reformatPath(str);
    assertEquals(result, "\\.\\^\\$\\{\\}\\+\\?\\(\\)\\[\\]\\|\\\\\\\\.*");
  }

  @Test
  public void toRegexExpr() {
    assertEquals("\\Qa.b.c\\E", StringUtils.toRegexExpr("a.b.c"));
    assertEquals(".*", StringUtils.toRegexExpr("*"));
    assertEquals(".*\\Q.b.c\\E", StringUtils.toRegexExpr("*.b.c"));
    assertEquals("\\Qa.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("a.*.c"));
    assertEquals("\\Qa.b.\\E.*", StringUtils.toRegexExpr("a.b.*"));
    assertEquals("\\Qa.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("a.*.*"));
    assertEquals(".*\\Q.b.\\E.*", StringUtils.toRegexExpr("*.b.*"));
    assertEquals(".*\\Q.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("*.*.c"));
    assertEquals(".*\\Q.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("*.*.*"));
    assertEquals(".*", StringUtils.toRegexExpr("**"));
    assertEquals(".*\\Q.b.c\\E", StringUtils.toRegexExpr("**.b.c"));
    assertEquals("\\Qa.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("a.**.c"));
    assertEquals("\\Qa.b.\\E.*", StringUtils.toRegexExpr("a.b.**"));
    assertEquals("\\Qa.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("a.**.**"));
    assertEquals(".*\\Q.b.\\E.*", StringUtils.toRegexExpr("**.b.**"));
    assertEquals(".*\\Q.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("**.**.c"));
    assertEquals(".*\\Q.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("**.**.**"));
  }

  @Test
  public void cutSchemaPrefix() {
    assertEquals(toSet("*"), StringUtils.cutSchemaPrefix(null, Collections.emptySet()));
    assertEquals(toSet("*"), StringUtils.cutSchemaPrefix("a.b", Collections.emptySet()));
    assertEquals(toSet("a.*", "b.*"), StringUtils.cutSchemaPrefix(null, toSet("a.*", "b.*")));
    assertEquals(toSet("*"), StringUtils.cutSchemaPrefix("a.b.c", toSet("a.*")));
    assertEquals(toSet("*"), StringUtils.cutSchemaPrefix("a.b.c", toSet("a.b.c.*")));
    assertEquals(Collections.emptySet(), StringUtils.cutSchemaPrefix("a.b.c", toSet("a.b.c")));
    assertEquals(Collections.emptySet(), StringUtils.cutSchemaPrefix("a.b.c", toSet("d.*")));
    assertEquals(Collections.emptySet(), StringUtils.cutSchemaPrefix("a.bb.c", toSet("a.b.*")));
    assertEquals(Collections.emptySet(), StringUtils.cutSchemaPrefix("a.b.c", toSet("a.bb.*")));
    assertEquals(toSet("*"), StringUtils.cutSchemaPrefix("a.b.c", toSet("*.b.*")));
    assertEquals(toSet("b.c", "*.b.c"), StringUtils.cutSchemaPrefix("a.b.c", toSet("*.b.c")));
  }

  @Test
  public void intersectDataPrefix() {
    assertEquals(
        toSet("a.b.c.*"), StringUtils.intersectDataPrefix(null, Collections.singleton("a.b.c.*")));
    assertEquals(
        toSet("a.b.*"), StringUtils.intersectDataPrefix("a.b", Collections.singleton("*")));
    assertEquals(
        toSet("a.b.c.*"), StringUtils.intersectDataPrefix("a.b", Collections.singleton("a.b.c.*")));
    assertEquals(
        toSet("a.b.c"), StringUtils.intersectDataPrefix("a.b", Collections.singleton("a.b.c")));
    assertEquals(
        toSet("a.b.*"), StringUtils.intersectDataPrefix("a.b", Collections.singleton("a.*")));
    assertEquals(
        toSet("a.b.c.*", "a.b.*.c.*"),
        StringUtils.intersectDataPrefix("a.b", Collections.singleton("*.c.*")));
    assertEquals(
        toSet("a.b.*"), StringUtils.intersectDataPrefix("a.b", Collections.singleton("*.b.*")));
    assertEquals(
        toSet("a.b.c", "a.b.b.c", "a.b.*.b.c"),
        StringUtils.intersectDataPrefix("a.b", Collections.singleton("*.b.c")));
    assertEquals(
        toSet("a.b.*.c"), StringUtils.intersectDataPrefix("a.b", Collections.singleton("*.b.*.c")));
    assertEquals(
        Collections.emptySet(),
        StringUtils.intersectDataPrefix("a.b", Collections.singleton("b.*.b.*.c")));
    assertEquals(
        Collections.emptySet(),
        StringUtils.intersectDataPrefix("a.b", Collections.singleton("a.c.b.*")));
  }

  private Set<String> toSet(String... items) {
    return new HashSet<>(Arrays.asList(items));
  }
}
