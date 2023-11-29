package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.SortedMap;

public interface DataIndex {

    /**
     * estimate size of data index
     *
     * @return size of data index in bytes
     */
    long size();

    /**
     * detect row ranges
     *
     * @param filter filter
     * @return row ranges, null if not supported. key is start row offset, value is end row offset.
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    SortedMap<Long, Long> detect(@Nonnull Filter filter) throws IOException;
}
