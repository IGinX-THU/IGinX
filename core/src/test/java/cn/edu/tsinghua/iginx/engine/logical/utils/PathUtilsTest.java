/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.logical.utils;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import org.junit.Test;

public class PathUtilsTest {

  @Test
  public void test() {
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
}
