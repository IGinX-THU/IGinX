/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.local.metadata;

import static org.apache.parquet.format.Util.readColumnMetaData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkProperties;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.local.ParquetMetadataConverter;
import org.apache.parquet.schema.PrimitiveType;

public class EncryptedColumnChunkMetaData extends ColumnChunkMetaData {
  private final ParquetMetadataConverter parquetMetadataConverter;
  private final byte[] encryptedMetadata;
  private final byte[] columnKeyMetadata;
  private final InternalFileDecryptor fileDecryptor;

  private final int columnOrdinal;
  private final PrimitiveType primitiveType;
  private final String createdBy;
  private ColumnPath path;

  private boolean decrypted;
  private ColumnChunkMetaData shadowColumnChunkMetaData;

  public EncryptedColumnChunkMetaData(
      ParquetMetadataConverter parquetMetadataConverter,
      ColumnPath path,
      PrimitiveType type,
      byte[] encryptedMetadata,
      byte[] columnKeyMetadata,
      InternalFileDecryptor fileDecryptor,
      int rowGroupOrdinal,
      int columnOrdinal,
      String createdBy) {
    super((EncodingStats) null, (ColumnChunkProperties) null);
    this.parquetMetadataConverter = parquetMetadataConverter;
    this.path = path;
    this.encryptedMetadata = encryptedMetadata;
    this.columnKeyMetadata = columnKeyMetadata;
    this.fileDecryptor = fileDecryptor;
    this.rowGroupOrdinal = rowGroupOrdinal;
    this.columnOrdinal = columnOrdinal;
    this.primitiveType = type;
    this.createdBy = createdBy;

    this.decrypted = false;
  }

  @Override
  protected void decryptIfNeeded() {
    if (decrypted) return;

    if (null == fileDecryptor) {
      throw new ParquetCryptoRuntimeException(path + ". Null File Decryptor");
    }

    // Decrypt the ColumnMetaData
    InternalColumnDecryptionSetup columnDecryptionSetup =
        fileDecryptor.setColumnCryptoMetadata(path, true, false, columnKeyMetadata, columnOrdinal);

    ColumnMetaData metaData;
    ByteArrayInputStream tempInputStream = new ByteArrayInputStream(encryptedMetadata);
    byte[] columnMetaDataAAD =
        AesCipher.createModuleAAD(
            fileDecryptor.getFileAAD(),
            ModuleType.ColumnMetaData,
            rowGroupOrdinal,
            columnOrdinal,
            -1);
    try {
      metaData =
          readColumnMetaData(
              tempInputStream, columnDecryptionSetup.getMetaDataDecryptor(), columnMetaDataAAD);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException(path + ". Failed to decrypt column metadata", e);
    }
    decrypted = true;
    shadowColumnChunkMetaData =
        parquetMetadataConverter.buildColumnChunkMetaData(metaData, path, primitiveType, createdBy);
    if (metaData.isSetBloom_filter_offset()) {
      setBloomFilterOffset(metaData.getBloom_filter_offset());
    }
  }

  @Override
  public ColumnPath getPath() {
    return path;
  }

  @Override
  public long getFirstDataPageOffset() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getFirstDataPageOffset();
  }

  @Override
  public long getDictionaryPageOffset() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getDictionaryPageOffset();
  }

  @Override
  public long getValueCount() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getValueCount();
  }

  @Override
  public long getTotalUncompressedSize() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getTotalUncompressedSize();
  }

  @Override
  public long getTotalSize() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getTotalSize();
  }

  @Override
  public Statistics getStatistics() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getStatistics();
  }

  @Override
  public boolean isEncrypted() {
    return true;
  }

  public CompressionCodecName getCodec() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getCodec();
  }

  @Override
  public PrimitiveType.PrimitiveTypeName getType() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getType();
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getPrimitiveType();
  }

  @Override
  public Set<Encoding> getEncodings() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getEncodings();
  }

  @Override
  public EncodingStats getEncodingStats() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getEncodingStats();
  }
}
