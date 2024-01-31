/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.db.common.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone.SerializeUtils;
import cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone.Tombstone;
import cn.edu.tsinghua.iginx.parquet.manager.data.LongFormat;
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
    Tombstone<Long, String> rb = new Tombstone<>();

    rb.delete(ImmutableRangeSet.of(Range.openClosed(1000L, 10000L)));
    rb.delete(Collections.singleton("full"), ImmutableRangeSet.of(Range.all()));
    rb.delete(Collections.singleton("empty"), ImmutableRangeSet.of());
    rb.delete(Collections.singleton("test"), ImmutableRangeSet.of(Range.closed(10L, 50L)));
    rb.delete(Collections.singleton("test2"), ImmutableRangeSet.of(Range.atLeast(10L)));
    rb.delete(ImmutableRangeSet.of(Range.closedOpen(1L, 100L)));
    rb.delete(Collections.singleton("test"), ImmutableRangeSet.of(Range.atLeast(10L)));
    rb.delete(Collections.singleton("test2"));
    rb.delete(Collections.singleton("test2"), ImmutableRangeSet.of(Range.atLeast(10L)));

    String str = SerializeUtils.serialize(rb, new LongFormat(), new StringFormat());
    Tombstone<Long, String> des =
        SerializeUtils.deserializeRangeTombstone(str, new LongFormat(), new StringFormat());

    assertEquals(rb, des);
  }

  @Test
  public void testEmptyTombstone() {
    Tombstone<Long, String> rb = new Tombstone<>();

    String str = SerializeUtils.serialize(rb, new LongFormat(), new StringFormat());
    Tombstone<Long, String> des =
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
