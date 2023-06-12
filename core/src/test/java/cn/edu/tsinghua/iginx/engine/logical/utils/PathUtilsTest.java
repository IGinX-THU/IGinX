package cn.edu.tsinghua.iginx.engine.logical.utils;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;
import org.junit.Test;

public class PathUtilsTest {

    @Test
    public void test() {
        ColumnsRange interval1 = new ColumnsInterval("*", "*");
        ColumnsRange expected1 = new ColumnsInterval(null, null);
        assertEquals(expected1, PathUtils.trimTimeSeriesInterval(interval1));

        ColumnsRange interval2 = new ColumnsInterval("a.*", "*.c");
        ColumnsRange expected2 = new ColumnsInterval("a.!", null);
        assertEquals(expected2, PathUtils.trimTimeSeriesInterval(interval2));

        ColumnsRange interval3 = new ColumnsInterval("*.d", "b.*");
        ColumnsRange expected3 = new ColumnsInterval(null, "b.~");
        assertEquals(expected3, PathUtils.trimTimeSeriesInterval(interval3));

        ColumnsRange interval4 = new ColumnsInterval("a.*.c", "b.*.c");
        ColumnsInterval expected4 = new ColumnsInterval("a.!", "b.~");
        assertEquals(expected4, PathUtils.trimTimeSeriesInterval(interval4));

        ColumnsInterval interval5 = new ColumnsInterval("a.*.*.c", "b.*.*.*.c");
        ColumnsInterval expected5 = new ColumnsInterval("a.!", "b.~");
        assertEquals(expected5, PathUtils.trimTimeSeriesInterval(interval5));
    }
}
