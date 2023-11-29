package cn.edu.tsinghua.iginx.parquet.entity;


import cn.edu.tsinghua.iginx.parquet.entity.Scanner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DataChunk extends AutoCloseable {

    /**
     * get the size of this data chunk
     *
     * @return size of this data chunk in bytes
     */
    long bytes();

    /**
     * get the number of rows in this data chunk
     *
     * @return number of rows in this data chunk
     */
    long rows();

    /**
     * get the data in specified row offset
     *
     * @return data in specified row offset, null if not existed
     */
    @Nullable
    Object get(long index);

    /**
     * get the scanner of this data chunk
     *
     * @param begin begin row offset
     * @param end end row offset
     * @return scanner of this data chunk
     */
    @Nonnull
    Scanner<Long, Object> scan(long begin, long end);
}
