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

package cn.edu.tsinghua.iginx.parquet.util;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * The properties of storage engine
 */
public class StorageProperties {
  private final long writeBufferSize;
  private final Duration writeBufferTimeout;
  private final long writeBatchSize;
  private final int compactPermits;
  private final long cacheCapacity;
  private final Duration cacheTimeout;
  private final boolean cacheSoftValues;
  private final boolean poolBufferRecycleEnable;
  private final int poolBufferRecycleAlign;
  private final int poolBufferRecycleLimit;
  private final long parquetRowGroupSize;
  private final int parquetPageSize;
  private final int parquetMaxOutputBufferSize;
  private final String parquetCompression;
  private final int zstdLevel;

  /**
   * Construct a StorageProperties
   *
   * @param writeBufferSize            the size of write buffer, bytes
   * @param writeBatchSize             the size of write batch, bytes
   * @param compactPermits             the number of flusher permits
   * @param cacheCapacity              the capacity of cache, bytes
   * @param cacheTimeout               the expiry timeout of cache
   * @param cacheSoftValues            whether to enable soft values of cache
   * @param parquetRowGroupSize        the size of parquet row group, bytes
   * @param parquetPageSize            the size of parquet page, bytes
   * @param parquetMaxOutputBufferSize the size of parquet max output buffer, bytes
   */
  private StorageProperties(
      long writeBufferSize,
      Duration writeBufferTimeout,
      long writeBatchSize,
      int compactPermits,
      long cacheCapacity,
      Duration cacheTimeout,
      boolean cacheSoftValues,
      boolean poolBufferRecycleEnable,
      int poolBufferRecycleAlign,
      int poolBufferRecycleLimit,
      long parquetRowGroupSize,
      int parquetPageSize,
      int parquetMaxOutputBufferSize,
      String parquetCompression,
      int zstdLevel) {
    this.writeBufferSize = writeBufferSize;
    this.writeBufferTimeout = writeBufferTimeout;
    this.writeBatchSize = writeBatchSize;
    this.compactPermits = compactPermits;
    this.cacheCapacity = cacheCapacity;
    this.cacheTimeout = cacheTimeout;
    this.cacheSoftValues = cacheSoftValues;
    this.poolBufferRecycleEnable = poolBufferRecycleEnable;
    this.poolBufferRecycleAlign = poolBufferRecycleAlign;
    this.poolBufferRecycleLimit = poolBufferRecycleLimit;
    this.parquetRowGroupSize = parquetRowGroupSize;
    this.parquetPageSize = parquetPageSize;
    this.parquetMaxOutputBufferSize = parquetMaxOutputBufferSize;
    this.parquetCompression = parquetCompression;
    this.zstdLevel = zstdLevel;
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
   * Get the timeout of write buffer to flush
   *
   * @return the timeout of write buffer to flush
   */
  public Duration getWriteBufferTimeout() {
    return writeBufferTimeout;
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
  public int getCompactPermits() {
    return compactPermits;
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
   * Get whether to enable pool buffer recycle
   *
   * @return whether to enable pool buffer recycle
   */
  public boolean getPoolBufferRecycleEnable() {
    return poolBufferRecycleEnable;
  }

  /**
   * Get the pool buffer recycle align
   *
   * @return the pool buffer recycle align
   */
  public int getPoolBufferRecycleAlign() {
    return poolBufferRecycleAlign;
  }

  /**
   * Get the pool buffer recycle limit
   *
   * @return the pool buffer recycle limit
   */
  public int getPoolBufferRecycleLimit() {
    return poolBufferRecycleLimit;
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
  public int getParquetPageSize() {
    return parquetPageSize;
  }

  /**
   * Get the size of parquet max output buffer in bytes
   *
   * @return the size of parquet max output buffer, bytes
   */
  public int getParquetMaxOutputBufferSize() {
    return parquetMaxOutputBufferSize;
  }

  /**
   * Get the parquet compression codec name
   *
   * @return the parquet compression codec name
   */
  public String getParquetCompression() {
    return parquetCompression;
  }

  /**
   * Get the zstd level
   *
   * @return the zstd level
   */
  public int getZstdLevel() {
    return zstdLevel;
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
        .add("writeBufferTimeout=" + writeBufferTimeout)
        .add("writeBatchSize=" + writeBatchSize)
        .add("compactPermits=" + compactPermits)
        .add("cacheCapacity=" + cacheCapacity)
        .add("cacheTimeout=" + cacheTimeout)
        .add("cacheSoftValues=" + cacheSoftValues)
        .add("poolBufferRecycleEnable=" + poolBufferRecycleEnable)
        .add("poolBufferRecycleAlign=" + poolBufferRecycleAlign)
        .add("poolBufferRecycleLimit=" + poolBufferRecycleLimit)
        .add("parquetRowGroupSize=" + parquetRowGroupSize)
        .add("parquetPageSize=" + parquetPageSize)
        .add("parquetMaxOutputBufferSize=" + parquetMaxOutputBufferSize)
        .add("parquetCompression='" + parquetCompression + "'")
        .add("zstdLevel=" + zstdLevel)
        .toString();
  }

  /**
   * A builder of StorageProperties
   */
  public static class Builder {
    public static final String WRITE_BUFFER_SIZE = "write.buffer.size";
    public static final String WRITE_BUFFER_TIMEOUT = "write.buffer.timeout";
    public static final String WRITE_BATCH_SIZE = "write.batch.size";
    public static final String COMPACT_PERMITS = "compact.permits";
    public static final String CACHE_CAPACITY = "cache.capacity";
    public static final String CACHE_TIMEOUT = "cache.timeout";
    public static final String CACHE_VALUE_SOFT = "cache.value.soft";
    public static final String POOL_BUFFER_RECYCLE_ENABLE = "pool.buffer.recycle.enable";
    public static final String POOL_BUFFER_RECYCLE_ALIGN = "pool.buffer.recycle.align";
    public static final String POOL_BUFFER_RECYCLE_LIMIT = "pool.buffer.recycle.limit";
    public static final String PARQUET_BLOCK_SIZE = "parquet.block.size";
    public static final String PARQUET_PAGE_SIZE = "parquet.page.size";
    public static final String PARQUET_MAX_OUTPUT_BUFFER_SIZE = "parquet.buffer.output.size.max";
    public static final String PARQUET_COMPRESSOR = "parquet.compression";
    public static final String ZSTD_LEVEL = "zstd.level";

    private long writeBufferSize = 100 * 1024 * 1024; // BYTE
    private Duration writeBufferTimeout = Duration.ofSeconds(0);
    private long writeBatchSize = 1024 * 1024; // BYTE
    private long cacheCapacity = 1024 * 1024 * 1024; // BYTE
    private Duration cacheTimeout = null;
    private boolean cacheSoftValues = false;
    private int compactPermits = 4;
    private boolean poolBufferRecycleEnable = true;
    private Integer poolBufferRecycleAlign = null;
    private Integer poolBufferRecycleLimit = null;
    private long parquetRowGroupSize = 128 * 1024 * 1024; // BYTE
    private int parquetPageSize = 8 * 1024; // BYTE
    private int parquetMaxOutputBufferSize = Integer.MAX_VALUE; // BYTE
    private String parquetCompression = "UNCOMPRESSED";
    private int zstdLevel = 3;

    private Builder() {
    }

    /**
     * Set the size of write buffer in bytes
     *
     * @param writeBufferSize the size of write buffer, bytes
     * @return this builder
     */
    public Builder setWriteBufferSize(long writeBufferSize) {
      ParseUtils.checkPositive(writeBufferSize);
      this.writeBufferSize = writeBufferSize;
      return this;
    }

    /**
     * Set the timeout of write buffer to flush
     *
     * @param writeBufferTimeout the timeout of write buffer to flush
     * @return this builder
     */
    public Builder setWriteBufferTimeout(Duration writeBufferTimeout) {
      this.writeBufferTimeout = writeBufferTimeout;
      return this;
    }

    /**
     * Set the size of write batch in bytes
     *
     * @param writeBatchSize the size of write batch, bytes
     * @return this builder
     */
    public Builder setWriteBatchSize(long writeBatchSize) {
      ParseUtils.checkPositive(writeBatchSize);
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
      ParseUtils.checkNonNegative(cacheCapacity);
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
      ParseUtils.checkPositive(cacheTimeout);
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
     * @param compactPermits the number of flusher permits
     * @return this builder
     */
    public Builder setCompactPermits(int compactPermits) {
      ParseUtils.checkPositive(compactPermits);
      this.compactPermits = compactPermits;
      return this;
    }

    /**
     * Set the pool buffer recycle enable
     *
     * @param poolBufferRecycleEnable the pool buffer recycle enable
     * @return this builder
     */
    public Builder setPoolBufferRecycleEnable(boolean poolBufferRecycleEnable) {
      this.poolBufferRecycleEnable = poolBufferRecycleEnable;
      return this;
    }

    /**
     * Set the pool buffer recycle align
     *
     * @param poolBufferRecycleAlign the pool buffer recycle align
     * @return this builder
     */
    public Builder setPoolBufferRecycleAlign(int poolBufferRecycleAlign) {
      this.poolBufferRecycleAlign = poolBufferRecycleAlign;
      return this;
    }

    /**
     * Set the pool buffer recycle limit
     *
     * @param poolBufferRecycleLimit the pool buffer recycle limit
     * @return this builder
     */
    public Builder setPoolBufferRecycleLimit(int poolBufferRecycleLimit) {
      this.poolBufferRecycleLimit = poolBufferRecycleLimit;
      return this;
    }

    /**
     * Set the size of parquet row group in bytes
     *
     * @param parquetRowGroupSize the size of parquet row group, bytes
     * @return this builder
     */
    public Builder setParquetRowGroupSize(long parquetRowGroupSize) {
      ParseUtils.checkPositive(parquetRowGroupSize);
      this.parquetRowGroupSize = parquetRowGroupSize;
      return this;
    }

    /**
     * Set the size of parquet page in bytes
     *
     * @param parquetPageSize the size of parquet page, bytes
     * @return this builder
     */
    public Builder setParquetPageSize(int parquetPageSize) {
      ParseUtils.checkPositive(parquetPageSize);
      this.parquetPageSize = parquetPageSize;
      return this;
    }

    /**
     * Set the size of parquet max output buffer in bytes
     *
     * @param parquetMaxOutputBufferSize the size of parquet max output buffer, bytes
     * @return this builder
     */
    public Builder setParquetMaxOutputBufferSize(int parquetMaxOutputBufferSize) {
      ParseUtils.checkPositive(parquetMaxOutputBufferSize);
      this.parquetMaxOutputBufferSize = parquetMaxOutputBufferSize;
      return this;
    }

    /**
     * Set the parquet compression codec name
     *
     * @param name the parquet compression codec name
     *             <p>Supported values: "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "ZSTD", "LZ4_RAW"
     * @return this builder
     */
    public Builder setParquetCompression(String name) {
      ParseUtils.in("UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "ZSTD", "LZ4_RAW").accept(name);
      this.parquetCompression = name;
      return this;
    }

    /**
     * Set the zstd level
     *
     * @param level the zstd level
     * @return this builder
     */
    public Builder setZstdLevel(int level) {
      this.zstdLevel = level;
      return this;
    }

    /**
     * Parse properties to set the properties of StorageProperties
     *
     * @param properties the properties to be parsed
     *                   <p>Supported keys:
     *                   <ul>
     *                     <li>write.buffer.size: the size of write buffer, bytes
     *                     <li>write.buffer.timeout: the timeout of write buffer to flush, iso-8601
     *                     <li>write.batch.size: the size of write batch, bytes
     *                     <li>compact.permits: the number of flusher permits
     *                     <li>cache.capacity: the capacity of cache, bytes
     *                     <li>cache.timeout: the expiry timeout of cache, iso8601 duration
     *                     <li>cache.value.soft: whether to enable soft values of cache
     *                     <li>pool.buffer.recycle.enable: the pool buffer recycle enable
     *                     <li>pool.buffer.recycle.align: the pool buffer recycle align
     *                     <li>pool.buffer.recycle.limit: the pool buffer recycle limit
     *                     <li>parquet.block.size: the size of parquet row group, bytes
     *                     <li>parquet.page.size: the size of parquet page, bytes
     *                     <li>parquet.buffer.output.size.max: the size of parquet max output buffer, bytes
     *                     <li>parquet.compression: the parquet compression codec name
     *                     <li>zstd.level: the zstd level
     *                   </ul>
     * @return this builder
     */
    public Builder parse(Map<String, String> properties) {
      ParseUtils.getOptionalLong(properties, WRITE_BUFFER_SIZE).ifPresent(this::setWriteBufferSize);
      ParseUtils.getOptionalDuration(properties, WRITE_BUFFER_TIMEOUT)
          .ifPresent(this::setWriteBufferTimeout);
      ParseUtils.getOptionalLong(properties, WRITE_BATCH_SIZE).ifPresent(this::setWriteBatchSize);
      ParseUtils.getOptionalInteger(properties, COMPACT_PERMITS).ifPresent(this::setCompactPermits);
      ParseUtils.getOptionalLong(properties, CACHE_CAPACITY).ifPresent(this::setCacheCapacity);
      ParseUtils.getOptionalDuration(properties, CACHE_TIMEOUT).ifPresent(this::setCacheTimeout);
      ParseUtils.getOptionalBoolean(properties, CACHE_VALUE_SOFT)
          .ifPresent(this::setCacheSoftValues);
      ParseUtils.getOptionalBoolean(properties, POOL_BUFFER_RECYCLE_ENABLE)
          .ifPresent(this::setPoolBufferRecycleEnable);
      ParseUtils.getOptionalInteger(properties, POOL_BUFFER_RECYCLE_ALIGN)
          .ifPresent(this::setPoolBufferRecycleAlign);
      ParseUtils.getOptionalInteger(properties, POOL_BUFFER_RECYCLE_LIMIT)
          .ifPresent(this::setPoolBufferRecycleLimit);
      ParseUtils.getOptionalLong(properties, PARQUET_BLOCK_SIZE)
          .ifPresent(this::setParquetRowGroupSize);
      ParseUtils.getOptionalInteger(properties, PARQUET_PAGE_SIZE)
          .ifPresent(this::setParquetPageSize);
      ParseUtils.getOptionalInteger(properties, PARQUET_MAX_OUTPUT_BUFFER_SIZE)
          .ifPresent(this::setParquetMaxOutputBufferSize);
      ParseUtils.getOptionalString(properties, PARQUET_COMPRESSOR)
          .ifPresent(this::setParquetCompression);
      ParseUtils.getOptionalInteger(properties, ZSTD_LEVEL).ifPresent(this::setZstdLevel);
      return this;
    }

    /**
     * Build a StorageProperties
     *
     * @return a StorageProperties
     */
    public StorageProperties build() {
      if (poolBufferRecycleAlign == null) {
        poolBufferRecycleAlign = Math.toIntExact(parquetPageSize);
      }
      if (poolBufferRecycleLimit == null) {
        poolBufferRecycleLimit = Math.toIntExact(parquetRowGroupSize);
      }
      return new StorageProperties(
          writeBufferSize,
          writeBufferTimeout,
          writeBatchSize,
          compactPermits,
          cacheCapacity,
          cacheTimeout,
          cacheSoftValues,
          poolBufferRecycleEnable,
          poolBufferRecycleAlign,
          poolBufferRecycleLimit,
          parquetRowGroupSize,
          parquetPageSize,
          parquetMaxOutputBufferSize,
          parquetCompression,
          zstdLevel);
    }
  }
}
