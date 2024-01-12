/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.common;

import java.util.Map;
import java.util.Optional;

public class Config {
  private final long writeBufferSize;
  private final long writeBatchSize;
  private final long parquetRowGroupSize;
  private final long parquetPageSize;

  private Config(
      long writeBufferSize, long writeBatchSize, long parquetRowGroupSize, long parquetPageSize) {
    this.writeBufferSize = writeBufferSize;
    this.writeBatchSize = writeBatchSize;
    this.parquetRowGroupSize = parquetRowGroupSize;
    this.parquetPageSize = parquetPageSize;
  }

  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  public long getWriteBatchSize() {
    return writeBatchSize;
  }

  public long getParquetRowGroupSize() {
    return parquetRowGroupSize;
  }

  public long getParquetPageSize() {
    return parquetPageSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    public static final String DATA_DIR = "dir";
    public static final String DUMMY_DIR = "dummy_dir";
    public static final String DUMMY_PREFIX = "embedded_prefix";
    public static final String WRITE_BUFFER_SIZE = "write_buffer_size";
    public static final String WRITE_BATCH_SIZE = "write_batch_size";
    public static final String PARQUET_ROW_GROUP_SIZE = "parquet.row_group_size";
    public static final String PARQUET_PAGE_SIZE = "parquet.page_size";

    private long writeBufferSize = 128 * 1024 * 1024; // BYTE
    private long writeBatchSize = 1024 * 1024; // BYTE
    private long parquetRowGroupSize = 128 * 1024 * 1024; // BYTE
    private long parquetPageSize = 1024 * 1024; // BYTE

    private Builder() {}

    public Builder setWriteBufferSize(long writeBufferSize) {
      this.writeBufferSize = writeBufferSize;
      return this;
    }

    public Builder setWriteBatchSize(long writeBatchSize) {
      this.writeBatchSize = writeBatchSize;
      return this;
    }

    public Builder setParquetRowGroupSize(long parquetRowGroupSize) {
      this.parquetRowGroupSize = parquetRowGroupSize;
      return this;
    }

    public Builder setParquetPageSize(long parquetPageSize) {
      this.parquetPageSize = parquetPageSize;
      return this;
    }

    public Builder parse(Map<String, String> properties) {
      getOptionalLong(properties, WRITE_BUFFER_SIZE).ifPresent(this::setWriteBufferSize);
      getOptionalLong(properties, WRITE_BATCH_SIZE).ifPresent(this::setWriteBatchSize);
      getOptionalLong(properties, PARQUET_ROW_GROUP_SIZE).ifPresent(this::setParquetRowGroupSize);
      getOptionalLong(properties, PARQUET_PAGE_SIZE).ifPresent(this::setParquetPageSize);
      return this;
    }

    public Config build() {
      check();
      return new Config(writeBufferSize, writeBatchSize, parquetRowGroupSize, parquetPageSize);
    }

    private void check() {
      // TODO: check
    }

    private static Optional<Long> getOptionalLong(Map<String, String> properties, String key) {
      String value = properties.get(key);
      if (value == null) {
        return Optional.empty();
      }
      return Optional.of(Long.parseLong(value));
    }
  }
}
