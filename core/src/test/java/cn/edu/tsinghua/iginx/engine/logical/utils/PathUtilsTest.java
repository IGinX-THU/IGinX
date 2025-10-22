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
package cn.edu.tsinghua.iginx.engine.logical.utils;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class PathUtilsTest {

  @Test
  public void testTrimColumnsInterval() {
    ColumnsInterval interval1 = new ColumnsInterval("*", "*");
    ColumnsInterval expected1 = new ColumnsInterval(null, null);
    assertEquals(expected1, PathUtils.trimColumnsInterval(interval1));

    ColumnsInterval interval2 = new ColumnsInterval("a.*", "*.c");
    ColumnsInterval expected2 = new ColumnsInterval("a.!", null);
    assertEquals(expected2, PathUtils.trimColumnsInterval(interval2));

    ColumnsInterval interval3 = new ColumnsInterval("*.d", "b.*");
    ColumnsInterval expected3 = new ColumnsInterval(null, "b.~");
    assertEquals(expected3, PathUtils.trimColumnsInterval(interval3));

    ColumnsInterval interval4 = new ColumnsInterval("a.*.c", "b.*.c");
    ColumnsInterval expected4 = new ColumnsInterval("a.!", "b.~");
    assertEquals(expected4, PathUtils.trimColumnsInterval(interval4));

    ColumnsInterval interval5 = new ColumnsInterval("a.*.*.c", "b.*.*.*.c");
    ColumnsInterval expected5 = new ColumnsInterval("a.!", "b.~");
    assertEquals(expected5, PathUtils.trimColumnsInterval(interval5));
  }

  @Test
  public void testRecoverRenamedPattern() {
    List<Pair<String, String>> aliasList1 = Collections.singletonList(new Pair<>("a.*", "b.*"));
    String pattern1 = "b.a.d";
    List<String> expected1 = Collections.singletonList("a.a.d");
    assertEquals(expected1, PathUtils.recoverRenamedPattern(aliasList1, pattern1));

    List<Pair<String, String>> aliasList2 =
        Arrays.asList(new Pair<>("k", "a.t1"), new Pair<>("v", "a.t2"));
    String pattern2 = "a.*";
    List<String> expected2 = Arrays.asList("k", "v");
    assertEquals(expected2, PathUtils.recoverRenamedPattern(aliasList2, pattern2));

    List<Pair<String, String>> aliasList3 =
        Arrays.asList(new Pair<>("k", "a.t1"), new Pair<>("v", "a.t2"));
    String pattern3 = "z";
    List<String> expected3 = Collections.singletonList("z");
    assertEquals(expected3, PathUtils.recoverRenamedPattern(aliasList3, pattern3));

    List<Pair<String, String>> aliasList4 = Collections.singletonList(new Pair<>("*", "a.*"));
    String pattern4 = "a.b.c";
    List<String> expected4 = Collections.singletonList("b.c");
    assertEquals(expected4, PathUtils.recoverRenamedPattern(aliasList4, pattern4));

    List<Pair<String, String>> aliasList5 =
        Collections.singletonList(new Pair<>("a\\parquet.c", "v"));
    String pattern5 = "v";
    List<String> expected5 = Collections.singletonList("a\\parquet.c");
    assertEquals(expected5, PathUtils.recoverRenamedPattern(aliasList5, pattern5));
  }
}
