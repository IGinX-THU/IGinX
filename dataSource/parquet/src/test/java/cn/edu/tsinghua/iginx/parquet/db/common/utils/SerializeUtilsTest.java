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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.parquet.db.common.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.manager.data.LongFormat;
import cn.edu.tsinghua.iginx.parquet.manager.data.SerializeUtils;
import cn.edu.tsinghua.iginx.parquet.manager.data.StringFormat;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Collections;
import org.junit.Test;

public class SerializeUtilsTest {

  @Test
  public void testSimpleTombstone() {
    AreaSet<Long, String> rb = new AreaSet<>();

    rb.add(ImmutableRangeSet.of(Range.openClosed(1000L, 10000L)));
    rb.add(Collections.singleton("full"), ImmutableRangeSet.of(Range.all()));
    rb.add(Collections.singleton("empty"), ImmutableRangeSet.of());
    rb.add(Collections.singleton("test"), ImmutableRangeSet.of(Range.closed(10L, 50L)));
    rb.add(Collections.singleton("test2"), ImmutableRangeSet.of(Range.atLeast(10L)));
    rb.add(ImmutableRangeSet.of(Range.closedOpen(1L, 100L)));
    rb.add(Collections.singleton("test"), ImmutableRangeSet.of(Range.atLeast(10L)));
    rb.add(Collections.singleton("test2"));
    rb.add(Collections.singleton("test2"), ImmutableRangeSet.of(Range.atLeast(10L)));

    String str = SerializeUtils.serialize(rb, new LongFormat(), new StringFormat());
    AreaSet<Long, String> des =
        SerializeUtils.deserializeRangeTombstone(str, new LongFormat(), new StringFormat());

    assertEquals(rb, des);
  }

  @Test
  public void testEmptyTombstone() {
    AreaSet<Long, String> rb = new AreaSet<>();

    String str = SerializeUtils.serialize(rb, new LongFormat(), new StringFormat());
    AreaSet<Long, String> des =
        SerializeUtils.deserializeRangeTombstone(str, new LongFormat(), new StringFormat());

    assertEquals(rb, des);
  }

  @Test
  public void testSimpleRangeSet() {
    RangeSet<Long> rs = TreeRangeSet.create();
    rs.add(Range.closed(1L, 100L));
    rs.add(Range.closed(1000L, 10000L));
    rs.add(Range.closed(100000L, 1000000L));

    String str = SerializeUtils.serialize(rs, new LongFormat());
    RangeSet<Long> des = SerializeUtils.deserializeRangeSet(str, new LongFormat());

    assertEquals(rs, des);
  }

  @Test
  public void testEmptyRangeSet() {
    RangeSet<Long> rs = TreeRangeSet.create();

    String str = SerializeUtils.serialize(rs, new LongFormat());
    RangeSet<Long> des = SerializeUtils.deserializeRangeSet(str, new LongFormat());

    assertEquals(rs, des);
  }
}
