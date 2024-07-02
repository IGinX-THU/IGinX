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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.util;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/** The properties of storage engine */
public class StorageProperties {
  private final boolean flushOnClose;
  private final long writeBufferSize;
  private final Duration writeBufferTimeout;
  private final long writeBatchSize;
  private final int compactPermits;
  private final long cacheCapacity;
  private final Duration cacheTimeout;
  private final boolean cacheSoftValues;
  private final long parquetRowGroupSize;
  private final long parquetPageSize;
  private final String parquetCompression;
  private final int zstdLevel;
  private final int zstdWorkers;
  private final int parquetLz4BufferSize;

  /**
   * Construct a StorageProperties
   *
   * @param flushOnClose whether to flush on close
   * @param writeBufferSize the size of write buffer, bytes
   * @param writeBatchSize the size of write batch, bytes
   * @param compactPermits the number of flusher permits
   * @param cacheCapacity the capacity of cache, bytes
   * @param cacheTimeout the expiry timeout of cache
   * @param cacheSoftValues whether to enable soft values of cache
   * @param parquetRowGroupSize the size of parquet row group, bytes
   * @param parquetPageSize the size of parquet page, bytes
   */
  private StorageProperties(
      boolean flushOnClose,
      long writeBufferSize,
      Duration writeBufferTimeout,
      long writeBatchSize,
      int compactPermits,
      long cacheCapacity,
      Duration cacheTimeout,
      boolean cacheSoftValues,
      long parquetRowGroupSize,
      long parquetPageSize,
      String parquetCompression,
      int zstdLevel,
      int zstdWorkers,
      int parquetLz4BufferSize) {
    this.flushOnClose = flushOnClose;
    this.writeBufferSize = writeBufferSize;
    this.writeBufferTimeout = writeBufferTimeout;
    this.writeBatchSize = writeBatchSize;
    this.compactPermits = compactPermits;
    this.cacheCapacity = cacheCapacity;
    this.cacheTimeout = cacheTimeout;
    this.cacheSoftValues = cacheSoftValues;
    this.parquetRowGroupSize = parquetRowGroupSize;
    this.parquetPageSize = parquetPageSize;
    this.parquetCompression = parquetCompression;
    this.zstdLevel = zstdLevel;
    this.zstdWorkers = zstdWorkers;
    this.parquetLz4BufferSize = parquetLz4BufferSize;
  }

  /**
   * Get whether to flush on close
   *
   * @return whether to flush on close
   */
  public boolean toFlushOnClose() {
    return flushOnClose;
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
   * Get the zstd workers number
   *
   * @return the zstd workers number
   */
  public int getZstdWorkers() {
    return zstdWorkers;
  }

  /**
   * Get the parquet lz4 buffer size
   *
   * @return the parquet lz4 buffer size
   */
  public int getParquetLz4BufferSize() {
    return parquetLz4BufferSize;
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
        .add("parquetRowGroupSize=" + parquetRowGroupSize)
        .add("parquetPageSize=" + parquetPageSize)
        .add("parquetCompression='" + parquetCompression + "'")
        .add("zstdLevel=" + zstdLevel)
        .add("zstdWorkers=" + zstdWorkers)
        .add("parquetLz4BufferSize=" + parquetLz4BufferSize)
        .toString();
  }

  /** A builder of StorageProperties */
  public static class Builder {
    public static final String FLUSH_ON_CLOSE = "close.flush";
    public static final String WRITE_BUFFER_SIZE = "write.buffer.size";
    public static final String WRITE_BUFFER_TIMEOUT = "write.buffer.timeout";
    public static final String WRITE_BATCH_SIZE = "write.batch.size";
    public static final String COMPACT_PERMITS = "compact.permits";
    public static final String CACHE_CAPACITY = "cache.capacity";
    public static final String CACHE_TIMEOUT = "cache.timeout";
    public static final String CACHE_VALUE_SOFT = "cache.value.soft";
    public static final String PARQUET_BLOCK_SIZE = "parquet.block.size";
    public static final String PARQUET_PAGE_SIZE = "parquet.page.size";
    public static final String PARQUET_COMPRESSOR = "parquet.compression";
    public static final String ZSTD_LEVEL = "zstd.level";
    public static final String ZSTD_WORKERS = "zstd.workers";
    public static final String PARQUET_LZ4_BUFFER_SIZE = "parquet.lz4.buffer.size";

    private boolean flushOnClose = true;
    private long writeBufferSize = 100 * 1024 * 1024; // BYTE
    private Duration writeBufferTimeout = Duration.ofSeconds(0);
    private long writeBatchSize = 1024 * 1024; // BYTE
    private long cacheCapacity = 16 * 1024 * 1024; // BYTE
    private Duration cacheTimeout = null;
    private boolean cacheSoftValues = false;
    private int compactPermits = 2;
    private long parquetRowGroupSize = 128 * 1024 * 1024; // BYTE
    private long parquetPageSize = 8 * 1024; // BYTE
    private String parquetCompression = "UNCOMPRESSED";
    private int zstdLevel = 3;
    private int zstdWorkers = 0;
    private int parquetLz4BufferSize = 256 * 1024; // BYTE

    private Builder() {}

    /**
     * Set whether to flush on close
     *
     * @param flushOnClose whether to flush on close
     * @return this builder
     */
    public Builder setFlushOnClose(boolean flushOnClose) {
      this.flushOnClose = flushOnClose;
      return this;
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
     * @param compactorPermits the number of flusher permits
     * @return this builder
     */
    public Builder setCompactorPermits(int compactorPermits) {
      ParseUtils.checkPositive(compactorPermits);
      this.compactPermits = compactorPermits;
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
    public Builder setParquetPageSize(long parquetPageSize) {
      ParseUtils.checkPositive(parquetPageSize);
      this.parquetPageSize = parquetPageSize;
      return this;
    }

    /**
     * Set the parquet compression codec name
     *
     * @param name the parquet compression codec name
     *     <p>Supported values: "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "ZSTD", "LZ4_RAW"
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
      ParseUtils.checkPositive(level);
      this.zstdLevel = level;
      return this;
    }

    /**
     * Set the zstd workers number
     *
     * @param workers the zstd workers number
     * @return this builder
     */
    public Builder setZstdWorkers(int workers) {
      ParseUtils.checkNonNegative(workers);
      this.zstdWorkers = workers;
      return this;
    }

    /**
     * Set the parquet lz4 buffer size
     *
     * @param bufferSize the parquet lz4 buffer size
     * @return this builder
     */
    public Builder setParquetLz4BufferSize(int bufferSize) {
      ParseUtils.checkPositive(bufferSize);
      this.parquetLz4BufferSize = bufferSize;
      return this;
    }

    /**
     * Parse properties to set the properties of StorageProperties
     *
     * @param properties the properties to be parsed
     *     <p>Supported keys:
     *     <ul>
     *       <li>close.flush: whether to flush on close
     *       <li>write.buffer.size: the size of write buffer, bytes
     *       <li>write.buffer.timeout: the timeout of write buffer to flush, iso-8601
     *       <li>write.batch.size: the size of write batch, bytes
     *       <li>compact.permits: the number of flusher permits
     *       <li>cache.capacity: the capacity of cache, bytes
     *       <li>cache.timeout: the expiry timeout of cache, iso8601 duration
     *       <li>cache.value.soft: whether to enable soft values of cache
     *       <li>parquet.block.size: the size of parquet row group, bytes
     *       <li>parquet.page.size: the size of parquet page, bytes
     *       <li>parquet.compression: the parquet compression codec name
     *       <li>parquet.lz4.buffer.size: the parquet lz4 buffer size, bytes
     *       <li>zstd.level: the zstd level
     *       <li>zstd.workers: the zstd workers number
     *     </ul>
     *
     * @return this builder
     */
    public Builder parse(Map<String, String> properties) {
      ParseUtils.getOptionalBoolean(properties, FLUSH_ON_CLOSE).ifPresent(this::setFlushOnClose);
      ParseUtils.getOptionalLong(properties, WRITE_BUFFER_SIZE).ifPresent(this::setWriteBufferSize);
      ParseUtils.getOptionalDuration(properties, WRITE_BUFFER_TIMEOUT)
          .ifPresent(this::setWriteBufferTimeout);
      ParseUtils.getOptionalLong(properties, WRITE_BATCH_SIZE).ifPresent(this::setWriteBatchSize);
      ParseUtils.getOptionalInteger(properties, COMPACT_PERMITS)
          .ifPresent(this::setCompactorPermits);
      ParseUtils.getOptionalLong(properties, CACHE_CAPACITY).ifPresent(this::setCacheCapacity);
      ParseUtils.getOptionalDuration(properties, CACHE_TIMEOUT).ifPresent(this::setCacheTimeout);
      ParseUtils.getOptionalBoolean(properties, CACHE_VALUE_SOFT)
          .ifPresent(this::setCacheSoftValues);
      ParseUtils.getOptionalLong(properties, PARQUET_BLOCK_SIZE)
          .ifPresent(this::setParquetRowGroupSize);
      ParseUtils.getOptionalLong(properties, PARQUET_PAGE_SIZE).ifPresent(this::setParquetPageSize);
      ParseUtils.getOptionalString(properties, PARQUET_COMPRESSOR)
          .ifPresent(this::setParquetCompression);
      ParseUtils.getOptionalInteger(properties, ZSTD_LEVEL).ifPresent(this::setZstdLevel);
      ParseUtils.getOptionalInteger(properties, ZSTD_WORKERS).ifPresent(this::setZstdWorkers);
      ParseUtils.getOptionalInteger(properties, PARQUET_LZ4_BUFFER_SIZE)
          .ifPresent(this::setParquetLz4BufferSize);
      return this;
    }

    /**
     * Build a StorageProperties
     *
     * @return a StorageProperties
     */
    public StorageProperties build() {
      return new StorageProperties(
          flushOnClose,
          writeBufferSize,
          writeBufferTimeout,
          writeBatchSize,
          compactPermits,
          cacheCapacity,
          cacheTimeout,
          cacheSoftValues,
          parquetRowGroupSize,
          parquetPageSize,
          parquetCompression,
          zstdLevel,
          zstdWorkers,
          parquetLz4BufferSize);
    }
  }
}
