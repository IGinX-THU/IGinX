package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.entity.DataChunk;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface FileReader extends Closeable {

    /**
     * set the row offset to read
     *
     * @param position row offset where start to read
     * @return true if seek successfully, false if not supported
     */
    boolean seek(long position) throws IOException;

    /**
     * load data chunks in current row offset
     *
     * @param fields fields to load. null if all fields
     * @param limit  max of row number to load
     * @return data chunks of each field
     */
    @Nonnull
    Map<String, DataChunk> load(@Nullable Set<String> fields, long limit) throws IOException;
}
