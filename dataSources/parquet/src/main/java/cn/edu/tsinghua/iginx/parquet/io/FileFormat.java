package cn.edu.tsinghua.iginx.parquet.io;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;

public interface FileFormat {

    /**
     * load meta data from file
     *
     * @param path file path
     * @return meta data
     * @throws IOException if an I/O error occurs
     */
    @Nonnull
    FileMeta getMeta(@Nonnull Path path) throws IOException;

    /**
     * load data index from file
     *
     * @param path file path
     * @return data index
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    DataIndex getIndex(@Nonnull Path path) throws IOException;

    /**
     * get reader of specified file
     *
     * @param path  file path
     * @param meta  meta data loaded from file
     * @param index data index loaded from file
     * @return file reader
     * @throws IOException if an I/O error occurs
     */
    @Nonnull
    FileReader getReader(@Nonnull Path path, @Nonnull FileMeta meta, @Nullable DataIndex index) throws IOException;

    /**
     * get writer of specified file
     *
     * @param path file path
     * @param meta meta data to write
     * @return file writer
     * @throws IOException if an I/O error occurs
     */
    @Nonnull
    FileWriter getWriter(Path path, @Nonnull FileMeta meta) throws IOException;
}
