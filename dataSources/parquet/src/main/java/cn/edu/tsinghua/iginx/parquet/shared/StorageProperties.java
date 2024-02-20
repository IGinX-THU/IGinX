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

package cn.edu.tsinghua.iginx.parquet.shared;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/** The properties of storage engine */
public class StorageProperties {
  private final long writeBufferSize;
  private final long writeBatchSize;
  private final int flusherPermits;
  private final long cacheCapacity;
  private final Duration cacheTimeout;
  private final boolean cacheSoftValues;
  private final long parquetRowGroupSize;
  private final long parquetPageSize;

  /**
   * Construct a StorageProperties
   *
   * @param writeBufferSize the size of write buffer, bytes
   * @param writeBatchSize the size of write batch, bytes
   * @param flusherPermits the number of flusher permits
   * @param cacheCapacity the capacity of cache, bytes
   * @param cacheTimeout the expiry timeout of cache
   * @param cacheSoftValues whether to enable soft values of cache
   * @param parquetRowGroupSize the size of parquet row group, bytes
   * @param parquetPageSize the size of parquet page, bytes
   */
  private StorageProperties(
      long writeBufferSize,
      long writeBatchSize,
      int flusherPermits,
      long cacheCapacity,
      Duration cacheTimeout,
      Boolean cacheSoftValues,
      long parquetRowGroupSize,
      long parquetPageSize) {
    this.writeBufferSize = writeBufferSize;
    this.writeBatchSize = writeBatchSize;
    this.flusherPermits = flusherPermits;
    this.cacheCapacity = cacheCapacity;
    this.cacheTimeout = cacheTimeout;
    this.cacheSoftValues = cacheSoftValues;
    this.parquetRowGroupSize = parquetRowGroupSize;
    this.parquetPageSize = parquetPageSize;
  }

  /**
   * Get the size of write buffer in bytes
   *
   * @return the size of write buffer, bytes
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Get the size of write batch in bytes
   *
   * @return the size of write batch, bytes
   */
  public long getWriteBatchSize() {
    return writeBatchSize;
  }

  /**
   * Get the shared permits allocator of flusher, which is used to control the total number of
   * flusher
   *
   * @return the shared permits allocator of flusher
   */
  public int getFlusherPermits() {
    return flusherPermits;
  }

  /**
   * Get the capacity of cache in bytes
   *
   * @return the capacity of cache, bytes
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * Get the expiry timeout of cache
   *
   * @return the expiry timeout of cache
   */
  public Optional<Duration> getCacheTimeout() {
    return Optional.ofNullable(cacheTimeout);
  }

  /**
   * Get whether to enable soft values of cache
   *
   * @return whether to enable soft values of cache
   */
  public boolean getCacheSoftValues() {
    return cacheSoftValues;
  }

  /**
   * Get the size of parquet row group in bytes
   *
   * @return the size of parquet row group, bytes
   */
  public long getParquetRowGroupSize() {
    return parquetRowGroupSize;
  }

  /**
   * Get the size of parquet page in bytes
   *
   * @return the size of parquet page, bytes
   */
  public long getParquetPageSize() {
    return parquetPageSize;
  }

  /**
   * Get a builder of StorageProperties
   *
   * @return a builder of StorageProperties
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", StorageProperties.class.getSimpleName() + "[", "]")
        .add("writeBufferSize=" + writeBufferSize)
        .add("writeBatchSize=" + writeBatchSize)
        .add("cacheCapacity=" + cacheCapacity)
        .add("cacheTimeout=" + cacheTimeout)
        .add("cacheSoftValues=" + cacheSoftValues)
        .add("flusherPermits=" + flusherPermits)
        .add("parquetRowGroupSize=" + parquetRowGroupSize)
        .add("parquetPageSize=" + parquetPageSize)
        .toString();
  }

  /** A builder of StorageProperties */
  public static class Builder {
    /** The property key of write buffer size */
    public static final String WRITE_BUFFER_SIZE = "write_buffer_size";
    /** The property key of write batch size */
    public static final String WRITE_BATCH_SIZE = "write_batch_size";
    /** The property key of flusher permits */
    public static final String FLUSHER_PERMITS = "flusher_permits";
    /** The property key of cache capacity */
    public static final String CACHE_CAPACITY = "cache.capacity";
    /** The property key of cache timeout */
    public static final String CACHE_TIMEOUT = "cache.timeout";
    /** The property key of whether to enable cache soft values */
    public static final String CACHE_SOFT_VALUES = "cache.soft_values";
    /** The property key of parquet row group size */
    public static final String PARQUET_ROW_GROUP_SIZE = "parquet.row_group_size";
    /** The property key of parquet page size */
    public static final String PARQUET_PAGE_SIZE = "parquet.page_size";

    private long writeBufferSize = 100 * 1024 * 1024; // BYTE
    private long writeBatchSize = 1024 * 1024; // BYTE

    private long cacheCapacity = 1024 * 1024 * 1024; // BYTE
    private Duration cacheTimeout = null;
    private boolean cacheSoftValues = false;
    private int flusherPermits = 16;
    private long parquetRowGroupSize = 128 * 1024 * 1024; // BYTE
    private long parquetPageSize = 8 * 1024; // BYTE

    private Builder() {}

    /**
     * Set the size of write buffer in bytes
     *
     * @param writeBufferSize the size of write buffer, bytes
     * @return this builder
     */
    public Builder setWriteBufferSize(long writeBufferSize) {
      this.writeBufferSize = writeBufferSize;
      return this;
    }

    /**
     * Set the size of write batch in bytes
     *
     * @param writeBatchSize the size of write batch, bytes
     * @return this builder
     */
    public Builder setWriteBatchSize(long writeBatchSize) {
      this.writeBatchSize = writeBatchSize;
      return this;
    }

    /**
     * Set the capacity of cache in bytes
     *
     * @param cacheCapacity the capacity of cache, bytes
     * @return this builder
     */
    public Builder setCacheCapacity(long cacheCapacity) {
      this.cacheCapacity = cacheCapacity;
      return this;
    }

    /**
     * Set the expiry timeout of cache
     *
     * @param cacheTimeout the expiry timeout of cache
     * @return this builder
     */
    public Builder setCacheTimeout(Duration cacheTimeout) {
      this.cacheTimeout = cacheTimeout;
      return this;
    }

    /**
     * Set whether to enable soft values of cache
     *
     * @param cacheSoftValues whether to enable soft values of cache
     * @return this builder
     */
    public Builder setCacheSoftValues(boolean cacheSoftValues) {
      this.cacheSoftValues = cacheSoftValues;
      return this;
    }

    /**
     * Set the number of flusher permits
     *
     * @param flusherPermits the number of flusher permits
     * @return this builder
     */
    public Builder setFlusherPermits(int flusherPermits) {
      this.flusherPermits = flusherPermits;
      return this;
    }

    /**
     * Set the size of parquet row group in bytes
     *
     * @param parquetRowGroupSize the size of parquet row group, bytes
     * @return this builder
     */
    public Builder setParquetRowGroupSize(long parquetRowGroupSize) {
      this.parquetRowGroupSize = parquetRowGroupSize;
      return this;
    }

    /**
     * Set the size of parquet page in bytes
     *
     * @param parquetPageSize the size of parquet page, bytes
     * @return this builder
     */
    public Builder setParquetPageSize(long parquetPageSize) {
      this.parquetPageSize = parquetPageSize;
      return this;
    }

    /**
     * Parse properties to set the properties of StorageProperties
     *
     * @param properties the properties to be parsed
     *     <p>"write_buffer_size": the size of write buffer,bytes, long, optional, default 100MB
     *     <p>"write_batch_size": the size of write batch, bytes, long, optional, default 1MB
     *     <p>"flusher_permits": the number of flusher permits, int, optional, default 16
     *     <p>"cache.capacity": the capacity of cache, bytes, long, optional, default 1GB
     *     <p>"cache.timeout": the expiry timeout of cache, iso-8601 duration format, optional,
     *     default Infinity
     *     <p>"cache.soft_values": whether to enable soft values of cache, boolean, optional,
     *     default false
     *     <p>"parquet.row_group_size": the size of parquet row group, bytes, long, optional,
     *     default 128MB
     *     <p>"parquet.page_size": the size of parquet page, bytes, long, optional, default 8KB
     * @return this builder
     */
    public Builder parse(Map<String, String> properties) {
      ParseUtils.getOptionalLong(properties, WRITE_BUFFER_SIZE, ParseUtils::checkPositive)
          .ifPresent(this::setWriteBufferSize);
      ParseUtils.getOptionalLong(properties, WRITE_BATCH_SIZE, ParseUtils::checkPositive)
          .ifPresent(this::setWriteBatchSize);
      ParseUtils.getOptionalInteger(properties, FLUSHER_PERMITS, ParseUtils::checkPositive)
          .ifPresent(this::setFlusherPermits);
      ParseUtils.getOptionalLong(properties, CACHE_CAPACITY, ParseUtils::checkNonNegative)
          .ifPresent(this::setCacheCapacity);
      ParseUtils.getOptionalDuration(properties, CACHE_TIMEOUT, ParseUtils::checkPositive)
          .ifPresent(this::setCacheTimeout);
      ParseUtils.getOptionalBoolean(properties, CACHE_SOFT_VALUES)
          .ifPresent(this::setCacheSoftValues);
      ParseUtils.getOptionalLong(properties, PARQUET_ROW_GROUP_SIZE, ParseUtils::checkPositive)
          .ifPresent(this::setParquetRowGroupSize);
      ParseUtils.getOptionalLong(properties, PARQUET_PAGE_SIZE, ParseUtils::checkPositive)
          .ifPresent(this::setParquetPageSize);
      return this;
    }

    /**
     * Build a StorageProperties
     *
     * @return a StorageProperties
     */
    public StorageProperties build() {
      return new StorageProperties(
          writeBufferSize,
          writeBatchSize,
          flusherPermits,
          cacheCapacity,
          cacheTimeout,
          cacheSoftValues,
          parquetRowGroupSize,
          parquetPageSize);
    }
  }
}
