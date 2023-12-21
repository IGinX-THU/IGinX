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

/*
 * copied from parquet-mr, updated by An Qi
 */

package org.apache.parquet.local;

import static org.apache.parquet.format.Util.writeFileCryptoMetaData;
import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.MAX_STATS_SIZE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.CRC32;
import org.apache.parquet.Preconditions;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.*;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.*;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Internal implementation of the Parquet file writer as a block container */
public class ParquetFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileWriter.class);

  private final ParquetMetadataConverter metadataConverter;

  public static final String PARQUET_METADATA_FILE = "_metadata";
  public static final String MAGIC_STR = "PAR1";
  public static final byte[] MAGIC = MAGIC_STR.getBytes(StandardCharsets.US_ASCII);
  public static final String EF_MAGIC_STR = "PARE";
  public static final byte[] EFMAGIC = EF_MAGIC_STR.getBytes(StandardCharsets.US_ASCII);
  public static final String PARQUET_COMMON_METADATA_FILE = "_common_metadata";
  public static final int CURRENT_VERSION = 1;

  // File creation modes
  public static enum Mode {
    CREATE,
    OVERWRITE
  }

  protected final PositionOutputStream out;

  private final AlignmentStrategy alignment;
  private final int columnIndexTruncateLength;

  // file data
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

  // The column/offset indexes per blocks per column chunks
  private final List<List<ColumnIndex>> columnIndexes = new ArrayList<>();
  private final List<List<OffsetIndex>> offsetIndexes = new ArrayList<>();

  // The Bloom filters
  private final List<Map<String, BloomFilter>> bloomFilters = new ArrayList<>();

  // The file encryptor
  private final InternalFileEncryptor fileEncryptor;

  // row group data
  private BlockMetaData currentBlock; // appended to by endColumn

  // The column/offset indexes for the actual block
  private List<ColumnIndex> currentColumnIndexes;
  private List<OffsetIndex> currentOffsetIndexes;

  // The Bloom filter for the actual block
  private Map<String, BloomFilter> currentBloomFilters;

  // row group data set at the start of a row group
  private long currentRecordCount; // set in startBlock

  // column chunk data accumulated as pages are written
  private EncodingStats.Builder encodingStatsBuilder;
  private Set<Encoding> currentEncodings;
  private long uncompressedLength;
  private long compressedLength;
  private Statistics currentStatistics; // accumulated in writePage(s)
  private ColumnIndexBuilder columnIndexBuilder;
  private OffsetIndexBuilder offsetIndexBuilder;

  // column chunk data set at the start of a column
  private CompressionCodecName currentChunkCodec; // set in startColumn
  private ColumnPath currentChunkPath; // set in startColumn
  private PrimitiveType currentChunkType; // set in startColumn
  private long currentChunkValueCount; // set in startColumn
  private long currentChunkFirstDataPage; // set in startColumn & page writes
  private long currentChunkDictionaryPageOffset; // set in writeDictionaryPage

  // set when end is called
  private ParquetMetadata footer = null;

  private final CRC32 crc;
  private boolean pageWriteChecksumEnabled;

  /** Captures the order in which methods should be called */
  private enum STATE {
    NOT_STARTED {
      STATE start() {
        return STARTED;
      }
    },
    STARTED {
      STATE startBlock() {
        return BLOCK;
      }

      STATE end() {
        return ENDED;
      }
    },
    BLOCK {
      STATE startColumn() {
        return COLUMN;
      }

      STATE endBlock() {
        return STARTED;
      }
    },
    COLUMN {
      STATE endColumn() {
        return BLOCK;
      };

      STATE write() {
        return this;
      }
    },
    ENDED;

    STATE start() throws IOException {
      return error();
    }

    STATE startBlock() throws IOException {
      return error();
    }

    STATE startColumn() throws IOException {
      return error();
    }

    STATE write() throws IOException {
      return error();
    }

    STATE endColumn() throws IOException {
      return error();
    }

    STATE endBlock() throws IOException {
      return error();
    }

    STATE end() throws IOException {
      return error();
    }

    private final STATE error() throws IOException {
      throw new IOException(
          "The file being written is in an invalid state. Probably caused by an error thrown previously. Current state: "
              + this.name());
    }
  }

  private STATE state = STATE.NOT_STARTED;

  public ParquetFileWriter(OutputFile file, ParquetWriteOptions options) throws IOException {
    this(
        file,
        options.isEnableOverwrite() ? Mode.OVERWRITE : Mode.CREATE,
        options.getRowGroupSize(),
        options.getMaxPaddingSize(),
        options.getParquetProperties().getColumnIndexTruncateLength(),
        options.getParquetProperties().getStatisticsTruncateLength(),
        options.getParquetProperties().getPageWriteChecksumEnabled(),
        options.getEncryptionProperties(),
        null);
  }

  private ParquetFileWriter(
      OutputFile file,
      Mode mode,
      long rowGroupSize,
      int maxPaddingSize,
      int columnIndexTruncateLength,
      int statisticsTruncateLength,
      boolean pageWriteChecksumEnabled,
      FileEncryptionProperties encryptionProperties,
      InternalFileEncryptor encryptor)
      throws IOException {

    long blockSize = rowGroupSize;
    if (file.supportsBlockSize()) {
      blockSize = Math.max(file.defaultBlockSize(), rowGroupSize);
      this.alignment = PaddingAlignment.get(blockSize, rowGroupSize, maxPaddingSize);
    } else {
      this.alignment = NoAlignment.get(rowGroupSize);
    }

    if (mode == Mode.OVERWRITE) {
      this.out = file.createOrOverwrite(blockSize);
    } else {
      this.out = file.create(blockSize);
    }

    this.encodingStatsBuilder = new EncodingStats.Builder();
    this.columnIndexTruncateLength = columnIndexTruncateLength;
    this.pageWriteChecksumEnabled = pageWriteChecksumEnabled;
    this.crc = pageWriteChecksumEnabled ? new CRC32() : null;

    this.metadataConverter = new ParquetMetadataConverter(statisticsTruncateLength);

    if (null == encryptionProperties && null == encryptor) {
      this.fileEncryptor = null;
      return;
    }

    if (null == encryptionProperties) {
      encryptionProperties = encryptor.getEncryptionProperties();
    }

    if (null == encryptor) {
      this.fileEncryptor = new InternalFileEncryptor(encryptionProperties);
    } else {
      this.fileEncryptor = encryptor;
    }
  }

  /**
   * start the file
   *
   * @throws IOException if there is an error while writing
   */
  public void start() throws IOException {
    state = state.start();
    LOG.debug("{}: start", out.getPos());
    byte[] magic = MAGIC;
    if (null != fileEncryptor && fileEncryptor.isFooterEncrypted()) {
      magic = EFMAGIC;
    }
    out.write(magic);
  }

  public InternalFileEncryptor getEncryptor() {
    return fileEncryptor;
  }

  /**
   * start a block
   *
   * @param recordCount the record count in this block
   * @throws IOException if there is an error while writing
   */
  public void startBlock(long recordCount) throws IOException {
    state = state.startBlock();
    LOG.debug("{}: start block", out.getPos());
    //    out.write(MAGIC); // TODO: add a magic delimiter

    alignment.alignForRowGroup(out);

    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;

    currentColumnIndexes = new ArrayList<>();
    currentOffsetIndexes = new ArrayList<>();

    currentBloomFilters = new HashMap<>();
  }

  /**
   * start a column inside a block
   *
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @param compressionCodecName a compression codec name
   * @throws IOException if there is an error while writing
   */
  public void startColumn(
      ColumnDescriptor descriptor, long valueCount, CompressionCodecName compressionCodecName)
      throws IOException {
    state = state.startColumn();
    encodingStatsBuilder.clear();
    currentEncodings = new HashSet<Encoding>();
    currentChunkPath = ColumnPath.get(descriptor.getPath());
    currentChunkType = descriptor.getPrimitiveType();
    currentChunkCodec = compressionCodecName;
    currentChunkValueCount = valueCount;
    currentChunkFirstDataPage = -1;
    compressedLength = 0;
    uncompressedLength = 0;
    // The statistics will be copied from the first one added at writeDataPage(s) so we have the
    // correct typed one
    currentStatistics = null;

    columnIndexBuilder = ColumnIndexBuilder.getBuilder(currentChunkType, columnIndexTruncateLength);
    offsetIndexBuilder = OffsetIndexBuilder.getBuilder();
  }

  /**
   * writes a dictionary page page
   *
   * @param dictionaryPage the dictionary page
   * @throws IOException if there is an error while writing
   */
  public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
    writeDictionaryPage(dictionaryPage, null, null);
  }

  public void writeDictionaryPage(
      DictionaryPage dictionaryPage, BlockCipher.Encryptor headerBlockEncryptor, byte[] AAD)
      throws IOException {
    state = state.write();
    LOG.debug(
        "{}: write dictionary page: {} values", out.getPos(), dictionaryPage.getDictionarySize());
    currentChunkDictionaryPageOffset = out.getPos();
    int uncompressedSize = dictionaryPage.getUncompressedSize();
    int compressedPageSize = (int) dictionaryPage.getBytes().size(); // TODO: fix casts
    if (pageWriteChecksumEnabled) {
      crc.reset();
      crc.update(dictionaryPage.getBytes().toByteArray());
      metadataConverter.writeDictionaryPageHeader(
          uncompressedSize,
          compressedPageSize,
          dictionaryPage.getDictionarySize(),
          dictionaryPage.getEncoding(),
          (int) crc.getValue(),
          out,
          headerBlockEncryptor,
          AAD);
    } else {
      metadataConverter.writeDictionaryPageHeader(
          uncompressedSize,
          compressedPageSize,
          dictionaryPage.getDictionarySize(),
          dictionaryPage.getEncoding(),
          out,
          headerBlockEncryptor,
          AAD);
    }
    long headerSize = out.getPos() - currentChunkDictionaryPageOffset;
    this.uncompressedLength += uncompressedSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    LOG.debug("{}: write dictionary page content {}", out.getPos(), compressedPageSize);
    dictionaryPage
        .getBytes()
        .writeAllTo(out); // for encrypted column, dictionary page bytes are already encrypted
    encodingStatsBuilder.addDictEncoding(dictionaryPage.getEncoding());
    currentEncodings.add(dictionaryPage.getEncoding());
  }

  /**
   * writes a single page
   *
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   * @throws IOException if there is an error while writing
   */
  @Deprecated
  public void writeDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding)
      throws IOException {
    state = state.write();
    // We are unable to build indexes without rowCount so skip them for this column
    offsetIndexBuilder = OffsetIndexBuilder.getNoOpBuilder();
    columnIndexBuilder = ColumnIndexBuilder.getNoOpBuilder();
    long beforeHeader = out.getPos();
    LOG.debug("{}: write data page: {} values", beforeHeader, valueCount);
    int compressedPageSize = (int) bytes.size();
    metadataConverter.writeDataPageV1Header(
        uncompressedPageSize,
        compressedPageSize,
        valueCount,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        out);
    long headerSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedPageSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    LOG.debug("{}: write data page content {}", out.getPos(), compressedPageSize);
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncoding(valuesEncoding);
    currentEncodings.add(rlEncoding);
    currentEncodings.add(dlEncoding);
    currentEncodings.add(valuesEncoding);
    if (currentChunkFirstDataPage < 0) {
      currentChunkFirstDataPage = beforeHeader;
    }
  }

  /**
   * writes a single page
   *
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param statistics statistics for the page
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   * @throws IOException if there is an error while writing
   * @deprecated this method does not support writing column indexes; Use {@link #writeDataPage(int,
   *     int, BytesInput, Statistics, long, Encoding, Encoding, Encoding)} instead
   */
  @Deprecated
  public void writeDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Statistics statistics,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding)
      throws IOException {
    // We are unable to build indexes without rowCount so skip them for this column
    offsetIndexBuilder = OffsetIndexBuilder.getNoOpBuilder();
    columnIndexBuilder = ColumnIndexBuilder.getNoOpBuilder();
    innerWriteDataPage(
        valueCount,
        uncompressedPageSize,
        bytes,
        statistics,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        null,
        null);
  }

  /**
   * Writes a single page
   *
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param statistics the statistics of the page
   * @param rowCount the number of rows in the page
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   * @throws IOException if any I/O error occurs during writing the file
   */
  public void writeDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Statistics statistics,
      long rowCount,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding)
      throws IOException {
    writeDataPage(
        valueCount,
        uncompressedPageSize,
        bytes,
        statistics,
        rowCount,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        null,
        null);
  }

  /**
   * Writes a single page
   *
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param statistics the statistics of the page
   * @param rowCount the number of rows in the page
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   * @param metadataBlockEncryptor encryptor for block data
   * @param pageHeaderAAD pageHeader AAD
   * @throws IOException if any I/O error occurs during writing the file
   */
  public void writeDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Statistics statistics,
      long rowCount,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding,
      BlockCipher.Encryptor metadataBlockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    long beforeHeader = out.getPos();
    innerWriteDataPage(
        valueCount,
        uncompressedPageSize,
        bytes,
        statistics,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        metadataBlockEncryptor,
        pageHeaderAAD);
    offsetIndexBuilder.add((int) (out.getPos() - beforeHeader), rowCount);
  }

  private void innerWriteDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Statistics statistics,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding,
      BlockCipher.Encryptor metadataBlockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    writeDataPage(
        valueCount,
        uncompressedPageSize,
        bytes,
        statistics,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        metadataBlockEncryptor,
        pageHeaderAAD);
  }

  /**
   * writes a single page
   *
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param statistics statistics for the page
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   * @param metadataBlockEncryptor encryptor for block data
   * @param pageHeaderAAD pageHeader AAD
   * @throws IOException if there is an error while writing
   */
  public void writeDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Statistics statistics,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding,
      BlockCipher.Encryptor metadataBlockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    state = state.write();
    long beforeHeader = out.getPos();
    if (currentChunkFirstDataPage < 0) {
      currentChunkFirstDataPage = beforeHeader;
    }
    LOG.debug("{}: write data page: {} values", beforeHeader, valueCount);
    int compressedPageSize = (int) bytes.size();
    if (pageWriteChecksumEnabled) {
      crc.reset();
      crc.update(bytes.toByteArray());
      metadataConverter.writeDataPageV1Header(
          uncompressedPageSize,
          compressedPageSize,
          valueCount,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          (int) crc.getValue(),
          out,
          metadataBlockEncryptor,
          pageHeaderAAD);
    } else {
      metadataConverter.writeDataPageV1Header(
          uncompressedPageSize,
          compressedPageSize,
          valueCount,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          out,
          metadataBlockEncryptor,
          pageHeaderAAD);
    }
    long headerSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedPageSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    LOG.debug("{}: write data page content {}", out.getPos(), compressedPageSize);
    bytes.writeAllTo(out);

    // Copying the statistics if it is not initialized yet so we have the correct typed one
    if (currentStatistics == null) {
      currentStatistics = statistics.copy();
    } else {
      currentStatistics.mergeStatistics(statistics);
    }

    columnIndexBuilder.add(statistics);

    encodingStatsBuilder.addDataEncoding(valuesEncoding);
    currentEncodings.add(rlEncoding);
    currentEncodings.add(dlEncoding);
    currentEncodings.add(valuesEncoding);
  }

  /**
   * Add a Bloom filter that will be written out. This is only used in unit test.
   *
   * @param column the column name
   * @param bloomFilter the bloom filter of column values
   */
  void addBloomFilter(String column, BloomFilter bloomFilter) {
    currentBloomFilters.put(column, bloomFilter);
  }

  /**
   * Writes a single v2 data page
   *
   * @param rowCount count of rows
   * @param nullCount count of nulls
   * @param valueCount count of values
   * @param repetitionLevels repetition level bytes
   * @param definitionLevels definition level bytes
   * @param dataEncoding encoding for data
   * @param compressedData compressed data bytes
   * @param uncompressedDataSize the size of uncompressed data
   * @param statistics the statistics of the page
   * @throws IOException if any I/O error occurs during writing the file
   */
  public void writeDataPageV2(
      int rowCount,
      int nullCount,
      int valueCount,
      BytesInput repetitionLevels,
      BytesInput definitionLevels,
      Encoding dataEncoding,
      BytesInput compressedData,
      int uncompressedDataSize,
      Statistics<?> statistics)
      throws IOException {
    state = state.write();
    int rlByteLength = toIntWithCheck(repetitionLevels.size());
    int dlByteLength = toIntWithCheck(definitionLevels.size());

    int compressedSize =
        toIntWithCheck(compressedData.size() + repetitionLevels.size() + definitionLevels.size());

    int uncompressedSize =
        toIntWithCheck(uncompressedDataSize + repetitionLevels.size() + definitionLevels.size());

    long beforeHeader = out.getPos();
    if (currentChunkFirstDataPage < 0) {
      currentChunkFirstDataPage = beforeHeader;
    }

    metadataConverter.writeDataPageV2Header(
        uncompressedSize,
        compressedSize,
        valueCount,
        nullCount,
        rowCount,
        dataEncoding,
        rlByteLength,
        dlByteLength,
        out);

    long headersSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedSize + headersSize;
    this.compressedLength += compressedSize + headersSize;

    if (currentStatistics == null) {
      currentStatistics = statistics.copy();
    } else {
      currentStatistics.mergeStatistics(statistics);
    }

    columnIndexBuilder.add(statistics);
    currentEncodings.add(dataEncoding);
    encodingStatsBuilder.addDataEncoding(dataEncoding);

    BytesInput.concat(repetitionLevels, definitionLevels, compressedData).writeAllTo(out);

    offsetIndexBuilder.add((int) (out.getPos() - beforeHeader), rowCount);
  }

  /**
   * Writes a column chunk at once
   *
   * @param descriptor the descriptor of the column
   * @param valueCount the value count in this column
   * @param compressionCodecName the name of the compression codec used for compressing the pages
   * @param dictionaryPage the dictionary page for this column chunk (might be null)
   * @param bytes the encoded pages including page headers to be written as is
   * @param uncompressedTotalPageSize total uncompressed size (without page headers)
   * @param compressedTotalPageSize total compressed size (without page headers)
   * @param totalStats accumulated statistics for the column chunk
   * @param columnIndexBuilder the builder object for the column index
   * @param offsetIndexBuilder the builder object for the offset index
   * @param bloomFilter the bloom filter for this column
   * @param rlEncodings the RL encodings used in this column chunk
   * @param dlEncodings the DL encodings used in this column chunk
   * @param dataEncodings the data encodings used in this column chunk
   * @throws IOException if there is an error while writing
   */
  void writeColumnChunk(
      ColumnDescriptor descriptor,
      long valueCount,
      CompressionCodecName compressionCodecName,
      DictionaryPage dictionaryPage,
      BytesInput bytes,
      long uncompressedTotalPageSize,
      long compressedTotalPageSize,
      Statistics<?> totalStats,
      ColumnIndexBuilder columnIndexBuilder,
      OffsetIndexBuilder offsetIndexBuilder,
      BloomFilter bloomFilter,
      Set<Encoding> rlEncodings,
      Set<Encoding> dlEncodings,
      List<Encoding> dataEncodings)
      throws IOException {
    writeColumnChunk(
        descriptor,
        valueCount,
        compressionCodecName,
        dictionaryPage,
        bytes,
        uncompressedTotalPageSize,
        compressedTotalPageSize,
        totalStats,
        columnIndexBuilder,
        offsetIndexBuilder,
        bloomFilter,
        rlEncodings,
        dlEncodings,
        dataEncodings,
        null,
        0,
        0,
        null);
  }

  void writeColumnChunk(
      ColumnDescriptor descriptor,
      long valueCount,
      CompressionCodecName compressionCodecName,
      DictionaryPage dictionaryPage,
      BytesInput bytes,
      long uncompressedTotalPageSize,
      long compressedTotalPageSize,
      Statistics<?> totalStats,
      ColumnIndexBuilder columnIndexBuilder,
      OffsetIndexBuilder offsetIndexBuilder,
      BloomFilter bloomFilter,
      Set<Encoding> rlEncodings,
      Set<Encoding> dlEncodings,
      List<Encoding> dataEncodings,
      BlockCipher.Encryptor headerBlockEncryptor,
      int rowGroupOrdinal,
      int columnOrdinal,
      byte[] fileAAD)
      throws IOException {
    startColumn(descriptor, valueCount, compressionCodecName);

    state = state.write();
    if (dictionaryPage != null) {
      byte[] dictonaryPageHeaderAAD = null;
      if (null != headerBlockEncryptor) {
        dictonaryPageHeaderAAD =
            AesCipher.createModuleAAD(
                fileAAD, ModuleType.DictionaryPageHeader, rowGroupOrdinal, columnOrdinal, -1);
      }
      writeDictionaryPage(dictionaryPage, headerBlockEncryptor, dictonaryPageHeaderAAD);
    }

    if (bloomFilter != null) {
      // write bloom filter if one of data pages is not dictionary encoded
      boolean isWriteBloomFilter = false;
      for (Encoding encoding : dataEncodings) {
        // dictionary encoding: `PLAIN_DICTIONARY` is used in parquet v1, `RLE_DICTIONARY` is used
        // in parquet v2
        if (encoding != Encoding.PLAIN_DICTIONARY && encoding != Encoding.RLE_DICTIONARY) {
          isWriteBloomFilter = true;
          break;
        }
      }
      if (isWriteBloomFilter) {
        currentBloomFilters.put(String.join(".", descriptor.getPath()), bloomFilter);
      }
    }
    LOG.debug("{}: write data pages", out.getPos());
    long headersSize = bytes.size() - compressedTotalPageSize;
    this.uncompressedLength += uncompressedTotalPageSize + headersSize;
    this.compressedLength += compressedTotalPageSize + headersSize;
    LOG.debug("{}: write data pages content", out.getPos());
    currentChunkFirstDataPage = out.getPos();
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncodings(dataEncodings);
    if (rlEncodings.isEmpty()) {
      encodingStatsBuilder.withV2Pages();
    }
    currentEncodings.addAll(rlEncodings);
    currentEncodings.addAll(dlEncodings);
    currentEncodings.addAll(dataEncodings);
    currentStatistics = totalStats;

    this.columnIndexBuilder = columnIndexBuilder;
    this.offsetIndexBuilder = offsetIndexBuilder;

    endColumn();
  }

  /**
   * end a column (once all rep, def and data have been written)
   *
   * @throws IOException if there is an error while writing
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    LOG.debug("{}: end column", out.getPos());
    if (columnIndexBuilder.getMinMaxSize() > columnIndexBuilder.getPageCount() * MAX_STATS_SIZE) {
      currentColumnIndexes.add(null);
    } else {
      currentColumnIndexes.add(columnIndexBuilder.build());
    }
    currentOffsetIndexes.add(offsetIndexBuilder.build(currentChunkFirstDataPage));
    currentBlock.addColumn(
        ColumnChunkMetaData.get(
            currentChunkPath,
            currentChunkType,
            currentChunkCodec,
            encodingStatsBuilder.build(),
            currentEncodings,
            currentStatistics,
            currentChunkFirstDataPage,
            currentChunkDictionaryPageOffset,
            currentChunkValueCount,
            compressedLength,
            uncompressedLength));
    this.currentBlock.setTotalByteSize(currentBlock.getTotalByteSize() + uncompressedLength);
    this.uncompressedLength = 0;
    this.compressedLength = 0;
    this.currentChunkDictionaryPageOffset = 0;
    columnIndexBuilder = null;
    offsetIndexBuilder = null;
  }

  /**
   * ends a block once all column chunks have been written
   *
   * @throws IOException if there is an error while writing
   */
  public void endBlock() throws IOException {
    if (currentRecordCount == 0) {
      throw new ParquetEncodingException("End block with zero record");
    }

    state = state.endBlock();
    LOG.debug("{}: end block", out.getPos());
    currentBlock.setRowCount(currentRecordCount);
    currentBlock.setOrdinal(blocks.size());
    blocks.add(currentBlock);
    columnIndexes.add(currentColumnIndexes);
    offsetIndexes.add(currentOffsetIndexes);
    bloomFilters.add(currentBloomFilters);
    currentColumnIndexes = null;
    currentOffsetIndexes = null;
    currentBloomFilters = null;
    currentBlock = null;
  }

  public void end(MessageType schema, Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    serializeColumnIndexes(columnIndexes, blocks, out, fileEncryptor);
    serializeOffsetIndexes(offsetIndexes, blocks, out, fileEncryptor);
    serializeBloomFilters(bloomFilters, blocks, out, fileEncryptor);
    LOG.debug("{}: end", out.getPos());
    this.footer =
        new ParquetMetadata(new FileMetaData(schema, extraMetaData, Version.FULL_VERSION), blocks);
    serializeFooter(footer, out, fileEncryptor, metadataConverter);
    out.close();
  }

  private static void serializeColumnIndexes(
      List<List<ColumnIndex>> columnIndexes,
      List<BlockMetaData> blocks,
      PositionOutputStream out,
      InternalFileEncryptor fileEncryptor)
      throws IOException {
    LOG.debug("{}: column indexes", out.getPos());
    for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
      BlockMetaData block = blocks.get(bIndex);
      List<ColumnChunkMetaData> columns = block.getColumns();
      List<ColumnIndex> blockColumnIndexes = columnIndexes.get(bIndex);
      for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
        ColumnChunkMetaData column = columns.get(cIndex);
        org.apache.parquet.format.ColumnIndex columnIndex =
            ParquetMetadataConverter.toParquetColumnIndex(
                column.getPrimitiveType(), blockColumnIndexes.get(cIndex));
        if (columnIndex == null) {
          continue;
        }
        BlockCipher.Encryptor columnIndexEncryptor = null;
        byte[] columnIndexAAD = null;
        if (null != fileEncryptor) {
          InternalColumnEncryptionSetup columnEncryptionSetup =
              fileEncryptor.getColumnSetup(column.getPath(), false, cIndex);
          if (columnEncryptionSetup.isEncrypted()) {
            columnIndexEncryptor = columnEncryptionSetup.getMetaDataEncryptor();
            columnIndexAAD =
                AesCipher.createModuleAAD(
                    fileEncryptor.getFileAAD(),
                    ModuleType.ColumnIndex,
                    block.getOrdinal(),
                    columnEncryptionSetup.getOrdinal(),
                    -1);
          }
        }
        long offset = out.getPos();
        Util.writeColumnIndex(columnIndex, out, columnIndexEncryptor, columnIndexAAD);
        column.setColumnIndexReference(new IndexReference(offset, (int) (out.getPos() - offset)));
      }
    }
  }

  private int toIntWithCheck(long size) {
    if ((int) size != size) {
      throw new ParquetEncodingException(
          "Cannot write page larger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int) size;
  }

  private static void serializeOffsetIndexes(
      List<List<OffsetIndex>> offsetIndexes,
      List<BlockMetaData> blocks,
      PositionOutputStream out,
      InternalFileEncryptor fileEncryptor)
      throws IOException {
    LOG.debug("{}: offset indexes", out.getPos());
    for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
      BlockMetaData block = blocks.get(bIndex);
      List<ColumnChunkMetaData> columns = block.getColumns();
      List<OffsetIndex> blockOffsetIndexes = offsetIndexes.get(bIndex);
      for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
        OffsetIndex offsetIndex = blockOffsetIndexes.get(cIndex);
        if (offsetIndex == null) {
          continue;
        }
        ColumnChunkMetaData column = columns.get(cIndex);
        BlockCipher.Encryptor offsetIndexEncryptor = null;
        byte[] offsetIndexAAD = null;
        if (null != fileEncryptor) {
          InternalColumnEncryptionSetup columnEncryptionSetup =
              fileEncryptor.getColumnSetup(column.getPath(), false, cIndex);
          if (columnEncryptionSetup.isEncrypted()) {
            offsetIndexEncryptor = columnEncryptionSetup.getMetaDataEncryptor();
            offsetIndexAAD =
                AesCipher.createModuleAAD(
                    fileEncryptor.getFileAAD(),
                    ModuleType.OffsetIndex,
                    block.getOrdinal(),
                    columnEncryptionSetup.getOrdinal(),
                    -1);
          }
        }
        long offset = out.getPos();
        Util.writeOffsetIndex(
            ParquetMetadataConverter.toParquetOffsetIndex(offsetIndex),
            out,
            offsetIndexEncryptor,
            offsetIndexAAD);
        column.setOffsetIndexReference(new IndexReference(offset, (int) (out.getPos() - offset)));
      }
    }
  }

  private static void serializeBloomFilters(
      List<Map<String, BloomFilter>> bloomFilters,
      List<BlockMetaData> blocks,
      PositionOutputStream out,
      InternalFileEncryptor fileEncryptor)
      throws IOException {
    LOG.debug("{}: bloom filters", out.getPos());
    for (int bIndex = 0, bSize = blocks.size(); bIndex < bSize; ++bIndex) {
      BlockMetaData block = blocks.get(bIndex);
      List<ColumnChunkMetaData> columns = block.getColumns();
      Map<String, BloomFilter> blockBloomFilters = bloomFilters.get(bIndex);
      if (blockBloomFilters.isEmpty()) continue;
      for (int cIndex = 0, cSize = columns.size(); cIndex < cSize; ++cIndex) {
        ColumnChunkMetaData column = columns.get(cIndex);
        BloomFilter bloomFilter = blockBloomFilters.get(column.getPath().toDotString());
        if (bloomFilter == null) {
          continue;
        }

        long offset = out.getPos();
        column.setBloomFilterOffset(offset);

        BlockCipher.Encryptor bloomFilterEncryptor = null;
        byte[] bloomFilterHeaderAAD = null;
        byte[] bloomFilterBitsetAAD = null;
        if (null != fileEncryptor) {
          InternalColumnEncryptionSetup columnEncryptionSetup =
              fileEncryptor.getColumnSetup(column.getPath(), false, cIndex);
          if (columnEncryptionSetup.isEncrypted()) {
            bloomFilterEncryptor = columnEncryptionSetup.getMetaDataEncryptor();
            int columnOrdinal = columnEncryptionSetup.getOrdinal();
            bloomFilterHeaderAAD =
                AesCipher.createModuleAAD(
                    fileEncryptor.getFileAAD(),
                    ModuleType.BloomFilterHeader,
                    block.getOrdinal(),
                    columnOrdinal,
                    -1);
            bloomFilterBitsetAAD =
                AesCipher.createModuleAAD(
                    fileEncryptor.getFileAAD(),
                    ModuleType.BloomFilterBitset,
                    block.getOrdinal(),
                    columnOrdinal,
                    -1);
          }
        }

        Util.writeBloomFilterHeader(
            ParquetMetadataConverter.toBloomFilterHeader(bloomFilter),
            out,
            bloomFilterEncryptor,
            bloomFilterHeaderAAD);

        ByteArrayOutputStream tempOutStream = new ByteArrayOutputStream();
        bloomFilter.writeTo(tempOutStream);
        byte[] serializedBitset = tempOutStream.toByteArray();
        if (null != bloomFilterEncryptor) {
          serializedBitset = bloomFilterEncryptor.encrypt(serializedBitset, bloomFilterBitsetAAD);
        }
        out.write(serializedBitset);
      }
    }
  }

  private static void serializeFooter(
      ParquetMetadata footer,
      PositionOutputStream out,
      InternalFileEncryptor fileEncryptor,
      ParquetMetadataConverter metadataConverter)
      throws IOException {

    // Unencrypted file
    if (null == fileEncryptor) {
      long footerIndex = out.getPos();
      org.apache.parquet.format.FileMetaData parquetMetadata =
          metadataConverter.toParquetMetadata(CURRENT_VERSION, footer);
      writeFileMetaData(parquetMetadata, out);
      LOG.debug("{}: footer length = {}", out.getPos(), (out.getPos() - footerIndex));
      BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
      out.write(MAGIC);
      return;
    }

    org.apache.parquet.format.FileMetaData parquetMetadata =
        metadataConverter.toParquetMetadata(CURRENT_VERSION, footer, fileEncryptor);

    // Encrypted file with plaintext footer
    if (!fileEncryptor.isFooterEncrypted()) {
      long footerIndex = out.getPos();
      parquetMetadata.setEncryption_algorithm(fileEncryptor.getEncryptionAlgorithm());
      // create footer signature (nonce + tag of encrypted footer)
      byte[] footerSigningKeyMetaData = fileEncryptor.getFooterSigningKeyMetaData();
      if (null != footerSigningKeyMetaData) {
        parquetMetadata.setFooter_signing_key_metadata(footerSigningKeyMetaData);
      }
      ByteArrayOutputStream tempOutStream = new ByteArrayOutputStream();
      writeFileMetaData(parquetMetadata, tempOutStream);
      byte[] serializedFooter = tempOutStream.toByteArray();
      byte[] footerAAD = AesCipher.createFooterAAD(fileEncryptor.getFileAAD());
      byte[] encryptedFooter =
          fileEncryptor.getSignedFooterEncryptor().encrypt(serializedFooter, footerAAD);
      byte[] signature = new byte[AesCipher.NONCE_LENGTH + AesCipher.GCM_TAG_LENGTH];
      System.arraycopy(
          encryptedFooter,
          ModuleCipherFactory.SIZE_LENGTH,
          signature,
          0,
          AesCipher.NONCE_LENGTH); // copy Nonce
      System.arraycopy(
          encryptedFooter,
          encryptedFooter.length - AesCipher.GCM_TAG_LENGTH,
          signature,
          AesCipher.NONCE_LENGTH,
          AesCipher.GCM_TAG_LENGTH); // copy GCM Tag
      out.write(serializedFooter);
      out.write(signature);
      LOG.debug("{}: footer and signature length = {}", out.getPos(), (out.getPos() - footerIndex));
      BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
      out.write(MAGIC);
      return;
    }

    // Encrypted file with encrypted footer
    long cryptoFooterIndex = out.getPos();
    writeFileCryptoMetaData(fileEncryptor.getFileCryptoMetaData(), out);
    byte[] footerAAD = AesCipher.createFooterAAD(fileEncryptor.getFileAAD());
    writeFileMetaData(parquetMetadata, out, fileEncryptor.getFooterEncryptor(), footerAAD);
    int combinedMetaDataLength = (int) (out.getPos() - cryptoFooterIndex);
    LOG.debug("{}: crypto metadata and footer length = {}", out.getPos(), combinedMetaDataLength);
    BytesUtils.writeIntLittleEndian(out, combinedMetaDataLength);
    out.write(EFMAGIC);
  }

  public ParquetMetadata getFooter() {
    Preconditions.checkState(state == STATE.ENDED, "Cannot return unfinished footer.");
    return footer;
  }

  /**
   * @return the current position in the underlying file
   * @throws IOException if there is an error while getting the current stream's position
   */
  public long getPos() throws IOException {
    return out.getPos();
  }

  public long getNextRowGroupSize() throws IOException {
    return alignment.nextRowGroupSize(out);
  }

  private interface AlignmentStrategy {
    void alignForRowGroup(PositionOutputStream out) throws IOException;

    long nextRowGroupSize(PositionOutputStream out) throws IOException;
  }

  private static class NoAlignment implements AlignmentStrategy {
    public static NoAlignment get(long rowGroupSize) {
      return new NoAlignment(rowGroupSize);
    }

    private final long rowGroupSize;

    private NoAlignment(long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
    }

    @Override
    public void alignForRowGroup(PositionOutputStream out) {}

    @Override
    public long nextRowGroupSize(PositionOutputStream out) {
      return rowGroupSize;
    }
  }

  /**
   * Alignment strategy that pads when less than half the row group size is left before the next DFS
   * block.
   */
  private static class PaddingAlignment implements AlignmentStrategy {
    private static final byte[] zeros = new byte[4096];

    public static PaddingAlignment get(long dfsBlockSize, long rowGroupSize, int maxPaddingSize) {
      return new PaddingAlignment(dfsBlockSize, rowGroupSize, maxPaddingSize);
    }

    protected final long dfsBlockSize;
    protected final long rowGroupSize;
    protected final int maxPaddingSize;

    private PaddingAlignment(long dfsBlockSize, long rowGroupSize, int maxPaddingSize) {
      this.dfsBlockSize = dfsBlockSize;
      this.rowGroupSize = rowGroupSize;
      this.maxPaddingSize = maxPaddingSize;
    }

    @Override
    public void alignForRowGroup(PositionOutputStream out) throws IOException {
      long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);

      if (isPaddingNeeded(remaining)) {
        LOG.debug(
            "Adding {} bytes of padding (row group size={}B, block size={}B)",
            remaining,
            rowGroupSize,
            dfsBlockSize);
        for (; remaining > 0; remaining -= zeros.length) {
          out.write(zeros, 0, (int) Math.min((long) zeros.length, remaining));
        }
      }
    }

    @Override
    public long nextRowGroupSize(PositionOutputStream out) throws IOException {
      if (maxPaddingSize <= 0) {
        return rowGroupSize;
      }

      long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);

      if (isPaddingNeeded(remaining)) {
        return rowGroupSize;
      }

      return Math.min(remaining, rowGroupSize);
    }

    protected boolean isPaddingNeeded(long remaining) {
      return (remaining <= maxPaddingSize);
    }
  }
}
