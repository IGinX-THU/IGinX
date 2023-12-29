/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.local;

import javax.annotation.Nullable;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetWriteOptions {

  // max size (bytes) to write as padding and the min size of a row group
  public static final int MAX_PADDING_SIZE_DEFAULT = 8 * 1024 * 1024; // 8MB
  public static final int ROW_GROUP_SIZE_DEFAULT = 128 * 1024 * 1024; // 128MB
  public static final boolean ENABLE_OVERWRITE_DEFAULT = true;
  public static final boolean ENABLE_VALIDATION_DEFAULT = false;

  private final ParquetProperties parquetProperties;

  private final boolean enableOverwrite;

  private final boolean enableValidation;

  private final long rowGroupSize;

  private final int maxPaddingSize;

  private final CompressionCodecFactory.BytesInputCompressor compressor;

  private final FileEncryptionProperties encryptionProperties;

  public ParquetWriteOptions(
      ParquetProperties parquetProperties,
      boolean enableOverwrite,
      boolean enableValidation,
      long rowGroupSize,
      int maxPaddingSize,
      CompressionCodecFactory.BytesInputCompressor compressor,
      FileEncryptionProperties encryptionProperties) {
    this.parquetProperties = parquetProperties;
    this.enableOverwrite = enableOverwrite;
    this.enableValidation = enableValidation;
    this.rowGroupSize = rowGroupSize;
    this.maxPaddingSize = maxPaddingSize;
    this.compressor = compressor;
    this.encryptionProperties = encryptionProperties;
  }

  public ParquetProperties getParquetProperties() {
    return parquetProperties;
  }

  public boolean isEnableOverwrite() {
    return enableOverwrite;
  }

  public boolean isEnableValidation() {
    return enableValidation;
  }

  public long getRowGroupSize() {
    return rowGroupSize;
  }

  public int getMaxPaddingSize() {
    return maxPaddingSize;
  }

  public CompressionCodecFactory.BytesInputCompressor getCompressor() {
    return compressor;
  }

  @Nullable
  public FileEncryptionProperties getEncryptionProperties() {
    return encryptionProperties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private ParquetProperties.Builder parquetPropertiesBuilder = ParquetProperties.builder();

    private CompressionCodecFactory.BytesInputCompressor compressor = null;

    private FileEncryptionProperties encryptionProperties = null;

    private long rowGroupSize = ROW_GROUP_SIZE_DEFAULT;

    private int maxPaddingSize = MAX_PADDING_SIZE_DEFAULT;

    private boolean enableValidation = ENABLE_VALIDATION_DEFAULT;

    private boolean enableOverwrite = ENABLE_OVERWRITE_DEFAULT;

    public Builder withOverwrite(boolean enableOverwrite) {
      this.enableOverwrite = enableOverwrite;
      return this;
    }

    public Builder withCompressor(CompressionCodecFactory.BytesInputCompressor compressor) {
      this.compressor = compressor;
      return this;
    }

    public Builder withEncryption(FileEncryptionProperties encryptionProperties) {
      this.encryptionProperties = encryptionProperties;
      return this;
    }

    public Builder withRowGroupSize(long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return this;
    }

    public Builder withPageSize(int pageSize) {
      parquetPropertiesBuilder.withPageSize(pageSize);
      return this;
    }

    public Builder withPageRowCountLimit(int rowCount) {
      parquetPropertiesBuilder.withPageRowCountLimit(rowCount);
      return this;
    }

    public Builder withDictionaryPageSize(int dictionaryPageSize) {
      parquetPropertiesBuilder.withDictionaryPageSize(dictionaryPageSize);
      return this;
    }

    public Builder withMaxPaddingSize(int maxPaddingSize) {
      this.maxPaddingSize = maxPaddingSize;
      return this;
    }

    public Builder withDictionaryEncoding(boolean enableDictionary) {
      parquetPropertiesBuilder.withDictionaryEncoding(enableDictionary);
      return this;
    }

    public Builder withByteStreamSplitEncoding(boolean enableByteStreamSplit) {
      parquetPropertiesBuilder.withByteStreamSplitEncoding(enableByteStreamSplit);
      return this;
    }

    public Builder withDictionaryEncoding(String columnPath, boolean enableDictionary) {
      parquetPropertiesBuilder.withDictionaryEncoding(columnPath, enableDictionary);
      return this;
    }

    public Builder withValidation(boolean enableValidation) {
      this.enableValidation = enableValidation;
      return this;
    }

    public Builder withWriterVersion(ParquetProperties.WriterVersion version) {
      parquetPropertiesBuilder.withWriterVersion(version);
      return this;
    }

    public Builder withPageWriteChecksumEnabled(boolean enablePageWriteChecksum) {
      parquetPropertiesBuilder.withPageWriteChecksumEnabled(enablePageWriteChecksum);
      return this;
    }

    public Builder withBloomFilterNDV(String columnPath, long ndv) {
      parquetPropertiesBuilder.withBloomFilterNDV(columnPath, ndv);
      return this;
    }

    public Builder withBloomFilterFPP(String columnPath, double fpp) {
      parquetPropertiesBuilder.withBloomFilterFPP(columnPath, fpp);
      return this;
    }

    public Builder withBloomFilterEnabled(boolean enabled) {
      parquetPropertiesBuilder.withBloomFilterEnabled(enabled);
      return this;
    }

    public Builder withBloomFilterEnabled(String columnPath, boolean enabled) {
      parquetPropertiesBuilder.withBloomFilterEnabled(columnPath, enabled);
      return this;
    }

    public Builder withMinRowCountForPageSizeCheck(int min) {
      parquetPropertiesBuilder.withMinRowCountForPageSizeCheck(min);
      return this;
    }

    public Builder withMaxRowCountForPageSizeCheck(int max) {
      parquetPropertiesBuilder.withMaxRowCountForPageSizeCheck(max);
      return this;
    }

    public Builder withColumnIndexTruncateLength(int length) {
      parquetPropertiesBuilder.withColumnIndexTruncateLength(length);
      return this;
    }

    public Builder withStatisticsTruncateLength(int length) {
      parquetPropertiesBuilder.withStatisticsTruncateLength(length);
      return this;
    }

    public Builder copy(ParquetWriteOptions options) {
      this.parquetPropertiesBuilder = ParquetProperties.copy(options.parquetProperties);
      withCompressor(options.compressor);
      withEncryption(options.encryptionProperties);
      withRowGroupSize(options.rowGroupSize);
      withMaxPaddingSize(options.maxPaddingSize);
      withValidation(options.enableValidation);
      withOverwrite(options.enableOverwrite);
      return this;
    }

    public ParquetWriteOptions build() {
      CompressionCodecFactory.BytesInputCompressor compressor = this.compressor;
      if (compressor == null) {
        compressor = new CodecFactory().getCompressor(CompressionCodecName.UNCOMPRESSED);
      }
      return new ParquetWriteOptions(
          parquetPropertiesBuilder.build(),
          enableOverwrite,
          enableValidation,
          rowGroupSize,
          maxPaddingSize,
          compressor,
          encryptionProperties);
    }
  }
}
