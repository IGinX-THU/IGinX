package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.io.*;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;

public class ParquetFormat implements FileFormat {
    private final ParquetReadOptions options;

    private final ParquetProperties properties;

    public ParquetFormat(ParquetReadOptions options, ParquetProperties properties) {
        this.options = options;
        this.properties = properties;
    }

    @Nonnull
    @Override
    public FileMeta getMeta(@Nonnull Path path) throws IOException {
        return new ParquetMeta(path, options);
    }

    @Override
    public DataIndex getIndex(@Nonnull Path path) throws IOException {
        return null;
    }

    @Nonnull
    @Override
    public FileReader getReader(@Nonnull Path path, @Nonnull FileMeta meta, @Nonnull DataIndex index) throws IOException {
        if (!(meta instanceof ParquetMeta)) {
            throw new FormatException("meta is not ParquetMeta");
        }

        return null;
    }

    @Nonnull
    @Override
    public FileWriter getWriter(Path path, @Nonnull FileMeta meta) throws IOException {
        return null;
    }

}
