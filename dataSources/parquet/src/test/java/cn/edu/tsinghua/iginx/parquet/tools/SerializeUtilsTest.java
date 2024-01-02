package cn.edu.tsinghua.iginx.parquet.tools;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.parquet.db.RangeTombstone;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.util.Collections;
import org.junit.Test;

public class SerializeUtilsTest {

  @Test
  public void testSimple() {
    RangeTombstone<Long, String> rb = new RangeTombstone<>();

    rb.delete(ImmutableRangeSet.of(Range.openClosed(1000L, 10000L)));
    rb.delete(Collections.singleton("full"), ImmutableRangeSet.of(Range.all()));
    rb.delete(Collections.singleton("empty"), ImmutableRangeSet.of());
    rb.delete(Collections.singleton("test"), ImmutableRangeSet.of(Range.closed(10L, 50L)));
    rb.delete(Collections.singleton("test2"), ImmutableRangeSet.of(Range.atLeast(10L)));
    rb.delete(ImmutableRangeSet.of(Range.closedOpen(1L, 100L)));
    rb.delete(Collections.singleton("test"), ImmutableRangeSet.of(Range.atLeast(10L)));
    rb.delete(Collections.singleton("test2"));
    rb.delete(Collections.singleton("test2"), ImmutableRangeSet.of(Range.atLeast(10L)));

    String str = SerializeUtils.serialize(rb);
    RangeTombstone<Long, String> des = SerializeUtils.deserializeRangeTombstone(str);

    assertEquals(rb, des);
  }
}
