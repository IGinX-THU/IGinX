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
package cn.edu.tsinghua.iginx.minio.common;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import com.google.common.collect.RangeSet;
import java.util.Arrays;
import org.junit.Test;

public class KeyRangesTest {

  @Test
  public void testAndOfKeyFilters() {
    RangeSet<Long> ranges =
        KeyRanges.of(
            new AndFilter(Arrays.asList(new KeyFilter(Op.GE, 10), new KeyFilter(Op.L, 20))));
    assertTrue(KeyRanges.intersects(ranges, 0, 10));
    assertTrue(KeyRanges.intersects(ranges, 19, 30));
    assertFalse(KeyRanges.intersects(ranges, 20, 30));
    assertFalse(KeyRanges.intersects(ranges, 0, 9));
  }

  @Test
  public void testOrKeepsEveryBranch() {
    RangeSet<Long> ranges =
        KeyRanges.of(new OrFilter(Arrays.asList(new KeyFilter(Op.L, 0), new KeyFilter(Op.G, 100))));
    assertTrue(KeyRanges.intersects(ranges, -5, -1));
    assertTrue(KeyRanges.intersects(ranges, 101, 200));
    assertFalse(KeyRanges.intersects(ranges, 0, 100));
  }

  @Test
  public void testValueFilterIsUnbounded() {
    Filter filter =
        new OrFilter(
            Arrays.asList(new KeyFilter(Op.E, 5), new ValueFilter("a.b", Op.G, new Value(1L))));
    RangeSet<Long> ranges = KeyRanges.of(filter);
    assertTrue(KeyRanges.intersects(ranges, 1000, 2000));
  }

  @Test
  public void testFalseFilterIsEmpty() {
    assertTrue(KeyRanges.of(new BoolFilter(false)).isEmpty());
    assertTrue(KeyRanges.intersects(KeyRanges.of(null), Long.MIN_VALUE, Long.MAX_VALUE));
  }
}
