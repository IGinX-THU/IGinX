/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet;

import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.data.DataManager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Shared;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.StorageProperties;
import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

@AutoService(FileStructure.class)
public class LegacyParquet implements FileStructure {

  public static final String NAME = "LegacyParquet";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Closeable newShared(Config config) throws IOException {
    StorageProperties.Builder builder = StorageProperties.builder();
    if (config.hasPath(StorageProperties.Builder.FLUSH_ON_CLOSE)) {
      builder.setFlushOnClose(config.getBoolean(StorageProperties.Builder.FLUSH_ON_CLOSE));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BUFFER_SIZE)) {
      builder.setWriteBufferSize(config.getBytes(StorageProperties.Builder.WRITE_BUFFER_SIZE));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BUFFER_PERMITS)) {
      builder.setWriteBufferPermits(config.getInt(StorageProperties.Builder.WRITE_BUFFER_PERMITS));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BUFFER_CHUNK_VALUES_MAX)) {
      builder.setWriteBufferChunkValuesMax(
          config.getInt(StorageProperties.Builder.WRITE_BUFFER_CHUNK_VALUES_MAX));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BUFFER_CHUNK_VALUES_MIN)) {
      builder.setWriteBufferChunkValuesMin(
          config.getInt(StorageProperties.Builder.WRITE_BUFFER_CHUNK_VALUES_MIN));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BUFFER_CHUNK_INDEX)) {
      builder.setWriteBufferChunkIndex(
          config.getString(StorageProperties.Builder.WRITE_BUFFER_CHUNK_INDEX));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BUFFER_TIMEOUT)) {
      builder.setWriteBufferTimeout(
          Duration.ofMillis(
              config.getDuration(StorageProperties.Builder.WRITE_BUFFER_TIMEOUT).toMillis()));
    }
    if (config.hasPath(StorageProperties.Builder.WRITE_BATCH_SIZE)) {
      builder.setWriteBatchSize(config.getBytes(StorageProperties.Builder.WRITE_BATCH_SIZE));
    }
    if (config.hasPath(StorageProperties.Builder.CACHE_CAPACITY)) {
      builder.setCacheCapacity(config.getBytes(StorageProperties.Builder.CACHE_CAPACITY));
    }
    if (config.hasPath(StorageProperties.Builder.CACHE_TIMEOUT)) {
      builder.setCacheTimeout(
          Duration.ofMillis(
              config.getDuration(StorageProperties.Builder.CACHE_TIMEOUT).toMillis()));
    }
    if (config.hasPath(StorageProperties.Builder.CACHE_VALUE_SOFT)) {
      builder.setCacheSoftValues(config.getBoolean(StorageProperties.Builder.CACHE_VALUE_SOFT));
    }
    if (config.hasPath(StorageProperties.Builder.COMPACT_PERMITS)) {
      builder.setCompactorPermits(config.getInt(StorageProperties.Builder.COMPACT_PERMITS));
    }
    if (config.hasPath(StorageProperties.Builder.PARQUET_BLOCK_SIZE)) {
      builder.setParquetRowGroupSize(config.getBytes(StorageProperties.Builder.PARQUET_BLOCK_SIZE));
    }
    if (config.hasPath(StorageProperties.Builder.PARQUET_PAGE_SIZE)) {
      builder.setParquetPageSize(config.getBytes(StorageProperties.Builder.PARQUET_PAGE_SIZE));
    }
    if (config.hasPath(StorageProperties.Builder.PARQUET_OUTPUT_BUFFER_SIZE)) {
      builder.setParquetOutputBufferMaxSize(
          Math.toIntExact(config.getBytes(StorageProperties.Builder.PARQUET_OUTPUT_BUFFER_SIZE)));
    }
    if (config.hasPath(StorageProperties.Builder.PARQUET_COMPRESSOR)) {
      builder.setParquetCompression(config.getString(StorageProperties.Builder.PARQUET_COMPRESSOR));
    }
    if (config.hasPath(StorageProperties.Builder.ZSTD_LEVEL)) {
      builder.setZstdLevel(config.getInt(StorageProperties.Builder.ZSTD_LEVEL));
    }
    if (config.hasPath(StorageProperties.Builder.ZSTD_WORKERS)) {
      builder.setZstdWorkers(config.getInt(StorageProperties.Builder.ZSTD_WORKERS));
    }
    if (config.hasPath(StorageProperties.Builder.PARQUET_LZ4_BUFFER_SIZE)) {
      builder.setParquetLz4BufferSize(
          Math.toIntExact(config.getBytes(StorageProperties.Builder.PARQUET_LZ4_BUFFER_SIZE)));
    }

    StorageProperties storageProperties = builder.build();
    return Shared.of(storageProperties);
  }

  @Override
  public FileManager newReader(Path path, Closeable shared) throws IOException {
    return new LegacyParquetWrapper(p -> new DataManager((Shared) shared, p), path, true);
  }

  @Override
  public boolean supportWrite() {
    return true;
  }

  @Override
  public FileManager newWriter(Path path, Closeable shared) throws IOException {
    return new LegacyParquetWrapper(p -> new DataManager((Shared) shared, p), path, false);
  }
}
