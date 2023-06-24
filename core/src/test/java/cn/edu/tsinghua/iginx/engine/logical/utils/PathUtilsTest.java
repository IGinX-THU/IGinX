package cn.edu.tsinghua.iginx.engine.logical.utils;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;
import org.junit.Test;

public class PathUtilsTest {

    @Test
    public void test() {
        ColumnsRange interval1 = new ColumnsRange("*", "*");
        ColumnsRange expected1 = new ColumnsRange(null, null);
        assertEquals(expected1, PathUtils.trimTimeSeriesInterval(interval1));

        ColumnsRange interval2 = new ColumnsRange("a.*", "*.c");
        ColumnsRange expected2 = new ColumnsRange("a.!", null);
        assertEquals(expected2, PathUtils.trimTimeSeriesInterval(interval2));

        ColumnsRange interval3 = new ColumnsRange("*.d", "b.*");
        ColumnsRange expected3 = new ColumnsRange(null, "b.~");
        assertEquals(expected3, PathUtils.trimTimeSeriesInterval(interval3));

        ColumnsRange interval4 = new ColumnsRange("a.*.c", "b.*.c");
        ColumnsRange expected4 = new ColumnsRange("a.!", "b.~");
        assertEquals(expected4, PathUtils.trimTimeSeriesInterval(interval4));

        ColumnsRange interval5 = new ColumnsRange("a.*.*.c", "b.*.*.*.c");
        ColumnsRange expected5 = new ColumnsRange("a.!", "b.~");
        assertEquals(expected5, PathUtils.trimTimeSeriesInterval(interval5));
    }
}
