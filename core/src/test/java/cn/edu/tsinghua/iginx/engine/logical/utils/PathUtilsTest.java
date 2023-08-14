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
