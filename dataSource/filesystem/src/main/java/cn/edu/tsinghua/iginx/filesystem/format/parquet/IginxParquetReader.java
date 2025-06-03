/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.ParquetReadOptions;
import shaded.iginx.org.apache.parquet.hadoop.ParquetFileReader;
import shaded.iginx.org.apache.parquet.hadoop.ParquetRecordReader;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.LocalInputFile;
import shaded.iginx.org.apache.parquet.io.SeekableInputStream;
import shaded.iginx.org.apache.parquet.schema.MessageType;

public class IginxParquetReader implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IginxParquetReader.class);

  private final ParquetRecordReader<IginxGroup> internalReader;

  public MessageType getProjectedSchema() {
    return projectedSchema;
  }

  private final MessageType projectedSchema;
  private final ParquetMetadata metadata;

  private IginxParquetReader(
      ParquetRecordReader<IginxGroup> internalReader,
      MessageType schema,
      ParquetMetadata metadata) {
    this.internalReader = internalReader;
    this.projectedSchema = schema;
    this.metadata = metadata;
  }

  public IginxGroup read() throws IOException {
    if (!internalReader.nextKeyValue()) {
      return null;
    }
    return internalReader.getCurrentValue();
  }

  @Override
  public void close() throws IOException {
    internalReader.close();
  }

  public long getCurrentRowIndex() {
    return internalReader.getCurrentRowIndex();
  }

  public static ParquetMetadata loadMetadata(Path path) throws IOException {
    return loadMetadata(path, ParquetReadOptions.builder().build());
  }

  public static ParquetMetadata loadMetadata(Path path, ParquetReadOptions options)
      throws IOException {
    LocalInputFile localInputFile = new LocalInputFile(path);
    try (SeekableInputStream in = localInputFile.newStream()) {
      return ParquetFileReader.readFooter(localInputFile, options, in);
    }
  }

  public static class Builder {

    private final ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
    private final Path path;
    private final ParquetMetadata metadata;
    private MessageType projection;

    public Builder(Path path, ParquetMetadata metadata) {
      this.path = Objects.requireNonNull(path);
      this.metadata = Objects.requireNonNull(metadata);
    }

    public IginxParquetReader build() throws IOException {
      MessageType requestedSchema =
          projection != null ? projection : metadata.getFileMetaData().getSchema();
      ParquetReadOptions options = optionsBuilder.build();
      ParquetFileReader reader = new ParquetFileReader(new LocalInputFile(path), metadata, options);
      reader.setRequestedSchema(requestedSchema);
      ParquetRecordReader<IginxGroup> internalReader =
          new ParquetRecordReader<>(new IginxRecordMaterializer(requestedSchema), reader, options);
      return new IginxParquetReader(internalReader, requestedSchema, metadata);
    }

    public Builder project(@Nullable MessageType projection) {
      this.projection = projection;
      return this;
    }

    @Override
    public String toString() {
      return "Builder{"
          + "optionsBuilder="
          + optionsBuilder
          + ", path="
          + path
          + ", metadata="
          + metadata
          + ", projection="
          + projection
          + '}';
    }
  }
}
