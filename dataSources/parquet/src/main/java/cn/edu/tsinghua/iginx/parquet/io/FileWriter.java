package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.entity.DataChunk;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface FileWriter extends Closeable {

    /**
     * set the row offset to write
     *
     * @param position row offset where start to write
     * @return true if seek successfully, false if not supported
     */
    boolean seek(long position) throws IOException;

    /**
     * dump data chunks into current row offset
     *
     * @param data   data chunks to dump. key is field name, value is data chunk.
     *               row number of each data chunk must be same.
     */
    void dump(Map<String, DataChunk> data) throws IOException;
}
