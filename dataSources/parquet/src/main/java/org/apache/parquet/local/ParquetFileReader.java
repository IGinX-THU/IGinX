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

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.format.Util.readFileCryptoMetaData;
import static org.apache.parquet.local.ColumnIndexFilterUtils.calculateOffsetRanges;
import static org.apache.parquet.local.ColumnIndexFilterUtils.filterOffsetIndex;
import static org.apache.parquet.local.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.local.ParquetFileWriter.MAGIC;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.*;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.*;
import org.apache.parquet.hadoop.ParquetEmptyBlockException;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.local.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.local.ColumnIndexFilterUtils.OffsetRange;
import org.apache.parquet.local.filter2.compat.RowGroupFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.yetus.audience.InterfaceAudience.Private;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Internal implementation of the Parquet file reader as a block container */
public class ParquetFileReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileReader.class);

  private final ParquetMetadataConverter converter;

  private final CRC32 crc;

  public static ParquetMetadata readFooter(
      InputFile file, ParquetReadOptions options, SeekableInputStream f) throws IOException {
    ParquetMetadataConverter converter = new ParquetMetadataConverter(options);
    return readFooter(file, options, f, converter);
  }

  private static ParquetMetadata readFooter(
      InputFile file,
      ParquetReadOptions options,
      SeekableInputStream f,
      ParquetMetadataConverter converter)
      throws IOException {

    long fileLen = file.getLength();
    String filePath = file.toString();
    LOG.debug("File length {}", fileLen);

    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen
        < MAGIC.length
            + FOOTER_LENGTH_SIZE
            + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(
          filePath + " is not a Parquet file (length is too low: " + fileLen + ")");
    }

    // Read footer length and magic string - with a single seek
    byte[] magic = new byte[MAGIC.length];
    long fileMetadataLengthIndex = fileLen - magic.length - FOOTER_LENGTH_SIZE;
    LOG.debug("reading footer index at {}", fileMetadataLengthIndex);
    f.seek(fileMetadataLengthIndex);
    int fileMetadataLength = readIntLittleEndian(f);
    f.readFully(magic);

    boolean encryptedFooterMode;
    if (Arrays.equals(MAGIC, magic)) {
      encryptedFooterMode = false;
    } else if (Arrays.equals(EFMAGIC, magic)) {
      encryptedFooterMode = true;
    } else {
      throw new RuntimeException(
          filePath
              + " is not a Parquet file. Expected magic number at tail, but found "
              + Arrays.toString(magic));
    }

    long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
    LOG.debug("read footer length: {}, footer index: {}", fileMetadataLength, fileMetadataIndex);
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException(
          "corrupted file: the footer index is not within the file: " + fileMetadataIndex);
    }
    f.seek(fileMetadataIndex);

    FileDecryptionProperties fileDecryptionProperties = options.getDecryptionProperties();
    InternalFileDecryptor fileDecryptor = null;
    if (null != fileDecryptionProperties) {
      fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
    }

    // Read all the footer bytes in one time to avoid multiple read operations,
    // since it can be pretty time consuming for a single read operation in HDFS.
    ByteBuffer footerBytesBuffer = ByteBuffer.allocate(fileMetadataLength);
    f.readFully(footerBytesBuffer);
    LOG.debug("Finished to read all footer bytes.");
    footerBytesBuffer.flip();
    InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);

    // Regular file, or encrypted file with plaintext footer
    if (!encryptedFooterMode) {
      return converter.readParquetMetadata(
          footerBytesStream, options.getMetadataFilter(), fileDecryptor, false, fileMetadataLength);
    }

    // Encrypted file with encrypted footer
    if (null == fileDecryptor) {
      throw new ParquetCryptoRuntimeException(
          "Trying to read file with encrypted footer. No keys available");
    }
    FileCryptoMetaData fileCryptoMetaData = readFileCryptoMetaData(footerBytesStream);
    fileDecryptor.setFileCryptoMetaData(
        fileCryptoMetaData.getEncryption_algorithm(), true, fileCryptoMetaData.getKey_metadata());
    // footer length is required only for signed plaintext footers
    return converter.readParquetMetadata(
        footerBytesStream, options.getMetadataFilter(), fileDecryptor, true, 0);
  }

  protected final SeekableInputStream f;
  private final InputFile file;
  private final ParquetReadOptions options;
  private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();
  private final FileMetaData fileMetaData; // may be null
  private final List<BlockMetaData> blocks;
  private final List<ColumnIndexStore> blockIndexStores;
  private final List<RowRanges> blockRowRanges;

  // not final. in some cases, this may be lazily loaded for backward-compat.
  private final ParquetMetadata footer;

  private int currentBlock = 0;
  private ColumnChunkPageReadStore currentRowGroup = null;
  private DictionaryPageReader nextDictionaryReader = null;

  private InternalFileDecryptor fileDecryptor = null;

  public ParquetFileReader(InputFile file, ParquetMetadata footer, ParquetReadOptions options)
      throws IOException {
    this.converter = new ParquetMetadataConverter(options);
    this.file = file;
    this.options = options;
    this.f = file.newStream();
    try {
      this.footer = footer;
      this.fileMetaData = footer.getFileMetaData();
      this.fileDecryptor =
          fileMetaData.getFileDecryptor(); // must be called before filterRowGroups!
      if (null != fileDecryptor && fileDecryptor.plaintextFile()) {
        this.fileDecryptor = null; // Plaintext file. No need in decryptor
      }

      this.blocks = filterRowGroups(footer.getBlocks());
      this.blockIndexStores = listWithNulls(this.blocks.size());
      this.blockRowRanges = listWithNulls(this.blocks.size());
      for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
        paths.put(ColumnPath.get(col.getPath()), col);
      }
      this.crc = options.usePageChecksumVerification() ? new CRC32() : null;
    } catch (Exception e) {
      f.close();
      throw e;
    }
  }

  private static <T> List<T> listWithNulls(int size) {
    return new ArrayList<>(Collections.nCopies(size, null));
  }

  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  public long getRecordCount() {
    long total = 0L;
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    return total;
  }

  public long getFilteredRecordCount() {
    if (!options.useColumnIndexFilter()
        || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return getRecordCount();
    }
    long total = 0L;
    for (int i = 0, n = blocks.size(); i < n; ++i) {
      total += getRowRanges(i).rowCount();
    }
    return total;
  }

  public String getFile() {
    return file.toString();
  }

  public List<BlockMetaData> filterRowGroups(List<BlockMetaData> blocks) throws IOException {
    FilterCompat.Filter recordFilter = options.getRecordFilter();
    if (FilterCompat.isFilteringRequired(recordFilter)) {
      // set up data filters based on configured levels
      List<RowGroupFilter.FilterLevel> levels = new ArrayList<>();

      if (options.useStatsFilter()) {
        levels.add(RowGroupFilter.FilterLevel.STATISTICS);
      }

      if (options.useDictionaryFilter()) {
        levels.add(RowGroupFilter.FilterLevel.DICTIONARY);
      }

      if (options.useBloomFilter()) {
        levels.add(RowGroupFilter.FilterLevel.BLOOMFILTER);
      }
      return RowGroupFilter.filterRowGroups(levels, recordFilter, blocks, this);
    }

    return blocks;
  }

  public List<BlockMetaData> getRowGroups() {
    return blocks;
  }

  private MessageType requestedSchema = null;

  public void setRequestedSchema(MessageType projection) {
    requestedSchema = projection;
    paths.clear();
    for (ColumnDescriptor col : projection.getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
  }

  public MessageType getRequestedSchema() {
    if (requestedSchema == null) {
      return fileMetaData.getSchema();
    }
    return requestedSchema;
  }

  /**
   * Reads all the columns requested from the row group at the specified block.
   *
   * @param blockIndex the index of the requested block
   * @return the PageReadStore which can provide PageReaders for each column.
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readRowGroup(int blockIndex) throws IOException {
    return internalReadRowGroup(blockIndex);
  }

  /**
   * Reads all the columns requested from the row group at the current file position.
   *
   * @return the PageReadStore which can provide PageReaders for each column.
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readNextRowGroup() throws IOException {
    ColumnChunkPageReadStore rowGroup = null;
    try {
      rowGroup = internalReadRowGroup(currentBlock);
    } catch (ParquetEmptyBlockException e) {
      LOG.warn("Read empty block at index {} from {}", currentBlock, getFile());
      advanceToNextBlock();
      return readNextRowGroup();
    }

    if (rowGroup == null) {
      return null;
    }
    this.currentRowGroup = rowGroup;
    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  private ColumnChunkPageReadStore internalReadRowGroup(int blockIndex) throws IOException {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(blockIndex);
    if (block.getRowCount() == 0) {
      throw new ParquetEmptyBlockException("Illegal row group of 0 rows");
    }
    org.apache.parquet.local.ColumnChunkPageReadStore rowGroup =
        new ColumnChunkPageReadStore(block.getRowCount(), block.getRowIndexOffset());
    // prepare the list of consecutive parts to read them in one scan
    List<ConsecutivePartList> allParts = new ArrayList<ConsecutivePartList>();
    ConsecutivePartList currentParts = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        long startingPos = mc.getStartingPos();
        // first part or not consecutive => new list
        if (currentParts == null || currentParts.endPos() != startingPos) {
          currentParts = new ConsecutivePartList(startingPos);
          allParts.add(currentParts);
        }
        currentParts.addChunk(
            new ChunkDescriptor(columnDescriptor, mc, startingPos, mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    ChunkListBuilder builder = new ChunkListBuilder(block.getRowCount());
    for (ConsecutivePartList consecutiveChunks : allParts) {
      consecutiveChunks.readAll(f, builder);
    }
    for (Chunk chunk : builder.build()) {
      readChunkPages(chunk, block, rowGroup);
    }

    return rowGroup;
  }

  /**
   * Reads all the columns requested from the specified row group. It may skip specific pages based
   * on the column indexes according to the actual filter. As the rows are not aligned among the
   * pages of the different columns row synchronization might be required. See the documentation of
   * the class SynchronizingColumnReader for details.
   *
   * @param blockIndex the index of the requested block
   * @return the PageReadStore which can provide PageReaders for each column or null if there are no
   *     rows in this block
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readFilteredRowGroup(int blockIndex) throws IOException {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }

    // Filtering not required -> fall back to the non-filtering path
    if (!options.useColumnIndexFilter()
        || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return internalReadRowGroup(blockIndex);
    }

    BlockMetaData block = blocks.get(blockIndex);
    if (block.getRowCount() == 0) {
      throw new ParquetEmptyBlockException("Illegal row group of 0 rows");
    }

    RowRanges rowRanges = getRowRanges(blockIndex);
    return readFilteredRowGroup(blockIndex, rowRanges);
  }

  /**
   * Reads all the columns requested from the specified row group. It may skip specific pages based
   * on the {@code rowRanges} passed in. As the rows are not aligned among the pages of the
   * different columns row synchronization might be required. See the documentation of the class
   * SynchronizingColumnReader for details.
   *
   * @param blockIndex the index of the requested block
   * @param rowRanges the row ranges to be read from the requested block
   * @return the PageReadStore which can provide PageReaders for each column or null if there are no
   *     rows in this block
   * @throws IOException if an error occurs while reading
   * @throws IllegalArgumentException if the {@code blockIndex} is invalid or the {@code rowRanges}
   *     is null
   */
  public ColumnChunkPageReadStore readFilteredRowGroup(int blockIndex, RowRanges rowRanges)
      throws IOException {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid block index %s, the valid block index range are: " + "[%s, %s]",
              blockIndex, 0, blocks.size() - 1));
    }

    if (Objects.isNull(rowRanges)) {
      throw new IllegalArgumentException("RowRanges must not be null");
    }

    BlockMetaData block = blocks.get(blockIndex);
    if (block.getRowCount() == 0L) {
      return null;
    }

    long rowCount = rowRanges.rowCount();
    if (rowCount == 0) {
      // There are no matching rows -> returning null
      return null;
    }

    if (rowCount == block.getRowCount()) {
      // All rows are matching -> fall back to the non-filtering path
      return internalReadRowGroup(blockIndex);
    }

    return internalReadFilteredRowGroup(block, rowRanges, getColumnIndexStore(blockIndex));
  }

  /**
   * Reads all the columns requested from the row group at the current file position. It may skip
   * specific pages based on the column indexes according to the actual filter. As the rows are not
   * aligned among the pages of the different columns row synchronization might be required. See the
   * documentation of the class SynchronizingColumnReader for details.
   *
   * @return the PageReadStore which can provide PageReaders for each column
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readNextFilteredRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    // Filtering not required -> fall back to the non-filtering path
    if (!options.useColumnIndexFilter()
        || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return readNextRowGroup();
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0L) {
      LOG.warn("Read empty block at index {} from {}", currentBlock, getFile());
      // Skip the empty block
      advanceToNextBlock();
      return readNextFilteredRowGroup();
    }
    RowRanges rowRanges = getRowRanges(currentBlock);
    long rowCount = rowRanges.rowCount();
    if (rowCount == 0) {
      // There are no matching rows -> skipping this row-group
      advanceToNextBlock();
      return readNextFilteredRowGroup();
    }
    if (rowCount == block.getRowCount()) {
      // All rows are matching -> fall back to the non-filtering path
      return readNextRowGroup();
    }

    this.currentRowGroup =
        internalReadFilteredRowGroup(block, rowRanges, getColumnIndexStore(currentBlock));

    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return this.currentRowGroup;
  }

  private ColumnChunkPageReadStore internalReadFilteredRowGroup(
      BlockMetaData block, RowRanges rowRanges, ColumnIndexStore ciStore) throws IOException {
    ColumnChunkPageReadStore rowGroup =
        new ColumnChunkPageReadStore(rowRanges, block.getRowIndexOffset());
    // prepare the list of consecutive parts to read them in one scan
    ChunkListBuilder builder = new ChunkListBuilder(block.getRowCount());
    List<ConsecutivePartList> allParts = new ArrayList<>();
    ConsecutivePartList currentParts = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        OffsetIndex offsetIndex = ciStore.getOffsetIndex(mc.getPath());

        OffsetIndex filteredOffsetIndex =
            filterOffsetIndex(offsetIndex, rowRanges, block.getRowCount());
        for (OffsetRange range :
            calculateOffsetRanges(filteredOffsetIndex, mc, offsetIndex.getOffset(0))) {
          long startingPos = range.getOffset();
          // first part or not consecutive => new list
          if (currentParts == null || currentParts.endPos() != startingPos) {
            currentParts = new ConsecutivePartList(startingPos);
            allParts.add(currentParts);
          }
          ChunkDescriptor chunkDescriptor =
              new ChunkDescriptor(columnDescriptor, mc, startingPos, range.getLength());
          currentParts.addChunk(chunkDescriptor);
          builder.setOffsetIndex(chunkDescriptor, filteredOffsetIndex);
        }
      }
    }
    // actually read all the chunks
    for (ConsecutivePartList consecutiveChunks : allParts) {
      consecutiveChunks.readAll(f, builder);
    }
    for (Chunk chunk : builder.build()) {
      readChunkPages(chunk, block, rowGroup);
    }

    return rowGroup;
  }

  private void readChunkPages(Chunk chunk, BlockMetaData block, ColumnChunkPageReadStore rowGroup)
      throws IOException {
    if (null == fileDecryptor || fileDecryptor.plaintextFile()) {
      rowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
      return;
    }
    // Encrypted file
    ColumnPath columnPath = ColumnPath.get(chunk.descriptor.col.getPath());
    InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(columnPath);
    if (!columnDecryptionSetup.isEncrypted()) { // plaintext column
      rowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
    } else { // encrypted column
      rowGroup.addColumn(
          chunk.descriptor.col,
          chunk.readAllPages(
              columnDecryptionSetup.getMetaDataDecryptor(),
              columnDecryptionSetup.getDataDecryptor(),
              fileDecryptor.getFileAAD(),
              block.getOrdinal(),
              columnDecryptionSetup.getOrdinal()));
    }
  }

  public ColumnIndexStore getColumnIndexStore(int blockIndex) {
    ColumnIndexStore ciStore = blockIndexStores.get(blockIndex);
    if (ciStore == null) {
      ciStore =
          org.apache.parquet.local.ColumnIndexStoreImpl.create(
              this, blocks.get(blockIndex), paths.keySet());
      blockIndexStores.set(blockIndex, ciStore);
    }
    return ciStore;
  }

  private RowRanges getRowRanges(int blockIndex) {
    assert FilterCompat.isFilteringRequired(options.getRecordFilter())
        : "Should not be invoked if filter is null or NOOP";
    RowRanges rowRanges = blockRowRanges.get(blockIndex);
    if (rowRanges == null) {
      rowRanges =
          ColumnIndexFilter.calculateRowRanges(
              options.getRecordFilter(),
              getColumnIndexStore(blockIndex),
              paths.keySet(),
              blocks.get(blockIndex).getRowCount());
      blockRowRanges.set(blockIndex, rowRanges);
    }
    return rowRanges;
  }

  public boolean skipNextRowGroup() {
    return advanceToNextBlock();
  }

  private boolean advanceToNextBlock() {
    if (currentBlock == blocks.size()) {
      return false;
    }

    // update the current block and instantiate a dictionary reader for it
    ++currentBlock;
    this.nextDictionaryReader = null;

    return true;
  }

  /**
   * Returns a {@link DictionaryPageReadStore} for the row group that would be returned by calling
   * {@link #readNextRowGroup()} or skipped by calling {@link #skipNextRowGroup()}.
   *
   * @return a DictionaryPageReadStore for the next row group
   */
  public DictionaryPageReadStore getNextDictionaryReader() {
    if (nextDictionaryReader == null) {
      this.nextDictionaryReader = getDictionaryReader(currentBlock);
    }
    return nextDictionaryReader;
  }

  public DictionaryPageReader getDictionaryReader(int blockIndex) {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }
    return new DictionaryPageReader(this, blocks.get(blockIndex));
  }

  public DictionaryPageReader getDictionaryReader(BlockMetaData block) {
    return new DictionaryPageReader(this, block);
  }

  /**
   * Reads and decompresses a dictionary page for the given column chunk.
   *
   * <p>Returns null if the given column chunk has no dictionary page.
   *
   * @param meta a column's ColumnChunkMetaData to read the dictionary from
   * @return an uncompressed DictionaryPage or null
   * @throws IOException if there is an error while reading the dictionary
   */
  DictionaryPage readDictionary(ColumnChunkMetaData meta) throws IOException {
    if (!meta.hasDictionaryPage()) {
      return null;
    }

    // TODO: this should use getDictionaryPageOffset() but it isn't reliable.
    if (f.getPos() != meta.getStartingPos()) {
      f.seek(meta.getStartingPos());
    }

    boolean encryptedColumn = false;
    InternalColumnDecryptionSetup columnDecryptionSetup = null;
    byte[] dictionaryPageAAD = null;
    BlockCipher.Decryptor pageDecryptor = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      columnDecryptionSetup = fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        encryptedColumn = true;
      }
    }

    PageHeader pageHeader;
    if (!encryptedColumn) {
      pageHeader = Util.readPageHeader(f);
    } else {
      byte[] dictionaryPageHeaderAAD =
          AesCipher.createModuleAAD(
              fileDecryptor.getFileAAD(),
              ModuleType.DictionaryPageHeader,
              meta.getRowGroupOrdinal(),
              columnDecryptionSetup.getOrdinal(),
              -1);
      pageHeader =
          Util.readPageHeader(
              f, columnDecryptionSetup.getMetaDataDecryptor(), dictionaryPageHeaderAAD);
      dictionaryPageAAD =
          AesCipher.createModuleAAD(
              fileDecryptor.getFileAAD(),
              ModuleType.DictionaryPage,
              meta.getRowGroupOrdinal(),
              columnDecryptionSetup.getOrdinal(),
              -1);
      pageDecryptor = columnDecryptionSetup.getDataDecryptor();
    }

    if (!pageHeader.isSetDictionary_page_header()) {
      return null; // TODO: should this complain?
    }

    DictionaryPage compressedPage =
        readCompressedDictionary(pageHeader, f, pageDecryptor, dictionaryPageAAD);
    BytesInputDecompressor decompressor =
        options.getCodecFactory().getDecompressor(meta.getCodec());

    return new DictionaryPage(
        decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
        compressedPage.getDictionarySize(),
        compressedPage.getEncoding());
  }

  private DictionaryPage readCompressedDictionary(
      PageHeader pageHeader,
      SeekableInputStream fin,
      BlockCipher.Decryptor pageDecryptor,
      byte[] dictionaryPageAAD)
      throws IOException {
    DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

    int uncompressedPageSize = pageHeader.getUncompressed_page_size();
    int compressedPageSize = pageHeader.getCompressed_page_size();

    byte[] dictPageBytes = new byte[compressedPageSize];
    fin.readFully(dictPageBytes);

    BytesInput bin = BytesInput.from(dictPageBytes);

    if (null != pageDecryptor) {
      bin = BytesInput.from(pageDecryptor.decrypt(bin.toByteArray(), dictionaryPageAAD));
    }

    return new DictionaryPage(
        bin,
        uncompressedPageSize,
        dictHeader.getNum_values(),
        converter.getEncoding(dictHeader.getEncoding()));
  }

  public BloomFilterReader getBloomFilterDataReader(int blockIndex) {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }
    return new BloomFilterReader(this, blocks.get(blockIndex));
  }

  public BloomFilterReader getBloomFilterDataReader(BlockMetaData block) {
    return new BloomFilterReader(this, block);
  }

  /**
   * Reads Bloom filter data for the given column chunk.
   *
   * @param meta a column's ColumnChunkMetaData to read the dictionary from
   * @return an BloomFilter object.
   * @throws IOException if there is an error while reading the Bloom filter.
   */
  public BloomFilter readBloomFilter(ColumnChunkMetaData meta) throws IOException {
    long bloomFilterOffset = meta.getBloomFilterOffset();
    if (bloomFilterOffset < 0) {
      return null;
    }

    // Prepare to decrypt Bloom filter (for encrypted columns)
    BlockCipher.Decryptor bloomFilterDecryptor = null;
    byte[] bloomFilterHeaderAAD = null;
    byte[] bloomFilterBitsetAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup =
          fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        bloomFilterDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        bloomFilterHeaderAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleType.BloomFilterHeader,
                meta.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
        bloomFilterBitsetAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleType.BloomFilterBitset,
                meta.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
      }
    }

    // Read Bloom filter data header.
    f.seek(bloomFilterOffset);
    BloomFilterHeader bloomFilterHeader;
    try {
      bloomFilterHeader = Util.readBloomFilterHeader(f, bloomFilterDecryptor, bloomFilterHeaderAAD);
    } catch (IOException e) {
      LOG.warn("read no bloom filter");
      return null;
    }

    int numBytes = bloomFilterHeader.getNumBytes();
    if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
      LOG.warn("the read bloom filter size is wrong, size is {}", bloomFilterHeader.getNumBytes());
      return null;
    }

    if (!bloomFilterHeader.getHash().isSetXXHASH()
        || !bloomFilterHeader.getAlgorithm().isSetBLOCK()
        || !bloomFilterHeader.getCompression().isSetUNCOMPRESSED()) {
      LOG.warn(
          "the read bloom filter is not supported yet,  algorithm = {}, hash = {}, compression = {}",
          bloomFilterHeader.getAlgorithm(),
          bloomFilterHeader.getHash(),
          bloomFilterHeader.getCompression());
      return null;
    }

    byte[] bitset;
    if (null == bloomFilterDecryptor) {
      bitset = new byte[numBytes];
      f.readFully(bitset);
    } else {
      bitset = bloomFilterDecryptor.decrypt(f, bloomFilterBitsetAAD);
      if (bitset.length != numBytes) {
        throw new ParquetCryptoRuntimeException("Wrong length of decrypted bloom filter bitset");
      }
    }
    return new BlockSplitBloomFilter(bitset);
  }

  /**
   * @param column the column chunk which the column index is to be returned for
   * @return the column index for the specified column chunk or {@code null} if there is no index
   * @throws IOException if any I/O error occurs during reading the file
   */
  @Private
  public ColumnIndex readColumnIndex(ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getColumnIndexReference();
    if (ref == null) {
      return null;
    }
    f.seek(ref.getOffset());

    BlockCipher.Decryptor columnIndexDecryptor = null;
    byte[] columnIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup =
          fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        columnIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        columnIndexAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleType.ColumnIndex,
                column.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
      }
    }
    return ParquetMetadataConverter.fromParquetColumnIndex(
        column.getPrimitiveType(), Util.readColumnIndex(f, columnIndexDecryptor, columnIndexAAD));
  }

  /**
   * @param column the column chunk which the offset index is to be returned for
   * @return the offset index for the specified column chunk or {@code null} if there is no index
   * @throws IOException if any I/O error occurs during reading the file
   */
  @Private
  public OffsetIndex readOffsetIndex(ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getOffsetIndexReference();
    if (ref == null) {
      return null;
    }
    f.seek(ref.getOffset());

    BlockCipher.Decryptor offsetIndexDecryptor = null;
    byte[] offsetIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup =
          fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        offsetIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        offsetIndexAAD =
            AesCipher.createModuleAAD(
                fileDecryptor.getFileAAD(),
                ModuleType.OffsetIndex,
                column.getRowGroupOrdinal(),
                columnDecryptionSetup.getOrdinal(),
                -1);
      }
    }
    return ParquetMetadataConverter.fromParquetOffsetIndex(
        Util.readOffsetIndex(f, offsetIndexDecryptor, offsetIndexAAD));
  }

  @Override
  public void close() throws IOException {
    try {
      if (f != null) {
        f.close();
      }
    } finally {
      options.getCodecFactory().release();
    }
  }

  public ParquetMetadata getFooter() {
    return footer;
  }

  /*
   * Builder to concatenate the buffers of the discontinuous parts for the same column. These parts are generated as a
   * result of the column-index based filtering when some pages might be skipped at reading.
   */
  private class ChunkListBuilder {
    private class ChunkData {
      final List<ByteBuffer> buffers = new ArrayList<>();
      OffsetIndex offsetIndex;
    }

    private final Map<ChunkDescriptor, ChunkData> map = new HashMap<>();
    private ChunkDescriptor lastDescriptor;
    private final long rowCount;
    private SeekableInputStream f;

    public ChunkListBuilder(long rowCount) {
      this.rowCount = rowCount;
    }

    void add(ChunkDescriptor descriptor, List<ByteBuffer> buffers, SeekableInputStream f) {
      map.computeIfAbsent(descriptor, d -> new ChunkData()).buffers.addAll(buffers);
      lastDescriptor = descriptor;
      this.f = f;
    }

    void setOffsetIndex(ChunkDescriptor descriptor, OffsetIndex offsetIndex) {
      map.computeIfAbsent(descriptor, d -> new ChunkData()).offsetIndex = offsetIndex;
    }

    List<Chunk> build() {
      Set<Entry<ChunkDescriptor, ChunkData>> entries = map.entrySet();
      List<Chunk> chunks = new ArrayList<>(entries.size());
      for (Entry<ChunkDescriptor, ChunkData> entry : entries) {
        ChunkDescriptor descriptor = entry.getKey();
        ChunkData data = entry.getValue();
        if (descriptor.equals(lastDescriptor)) {
          // because of a bug, the last chunk might be larger than descriptor.size
          chunks.add(
              new WorkaroundChunk(lastDescriptor, data.buffers, f, data.offsetIndex, rowCount));
        } else {
          chunks.add(new Chunk(descriptor, data.buffers, data.offsetIndex, rowCount));
        }
      }
      return chunks;
    }
  }

  /** The data for a column chunk */
  private class Chunk {

    protected final ChunkDescriptor descriptor;
    protected final ByteBufferInputStream stream;
    final OffsetIndex offsetIndex;
    final long rowCount;

    /**
     * @param descriptor descriptor for the chunk
     * @param buffers ByteBuffers that contain the chunk
     * @param offsetIndex the offset index for this column; might be null
     */
    public Chunk(
        ChunkDescriptor descriptor,
        List<ByteBuffer> buffers,
        OffsetIndex offsetIndex,
        long rowCount) {
      this.descriptor = descriptor;
      this.stream = ByteBufferInputStream.wrap(buffers);
      this.offsetIndex = offsetIndex;
      this.rowCount = rowCount;
    }

    protected PageHeader readPageHeader() throws IOException {
      return readPageHeader(null, null);
    }

    protected PageHeader readPageHeader(BlockCipher.Decryptor blockDecryptor, byte[] pageHeaderAAD)
        throws IOException {
      return Util.readPageHeader(stream, blockDecryptor, pageHeaderAAD);
    }

    /**
     * Calculate checksum of input bytes, throw decoding exception if it does not match the provided
     * reference crc
     */
    private void verifyCrc(int referenceCrc, byte[] bytes, String exceptionMsg) {
      crc.reset();
      crc.update(bytes);
      if (crc.getValue() != ((long) referenceCrc & 0xffffffffL)) {
        throw new ParquetDecodingException(exceptionMsg);
      }
    }

    /**
     * Read all of the pages in a given column chunk.
     *
     * @return the list of pages
     */
    public ColumnChunkPageReader readAllPages() throws IOException {
      return readAllPages(null, null, null, -1, -1);
    }

    public ColumnChunkPageReader readAllPages(
        BlockCipher.Decryptor headerBlockDecryptor,
        BlockCipher.Decryptor pageBlockDecryptor,
        byte[] aadPrefix,
        int rowGroupOrdinal,
        int columnOrdinal)
        throws IOException {
      List<DataPage> pagesInChunk = new ArrayList<>();
      DictionaryPage dictionaryPage = null;
      PrimitiveType type =
          getFileMetaData().getSchema().getType(descriptor.col.getPath()).asPrimitiveType();
      long valuesCountReadSoFar = 0L;
      int dataPageCountReadSoFar = 0;
      byte[] dataPageHeaderAAD = null;
      if (null != headerBlockDecryptor) {
        dataPageHeaderAAD =
            AesCipher.createModuleAAD(
                aadPrefix,
                ModuleType.DataPageHeader,
                rowGroupOrdinal,
                columnOrdinal,
                getPageOrdinal(dataPageCountReadSoFar));
      }
      while (hasMorePages(valuesCountReadSoFar, dataPageCountReadSoFar)) {
        byte[] pageHeaderAAD = dataPageHeaderAAD;
        if (null != headerBlockDecryptor) {
          // Important: this verifies file integrity (makes sure dictionary page had not been
          // removed)
          if (null == dictionaryPage && descriptor.metadata.hasDictionaryPage()) {
            pageHeaderAAD =
                AesCipher.createModuleAAD(
                    aadPrefix, ModuleType.DictionaryPageHeader, rowGroupOrdinal, columnOrdinal, -1);
          } else {
            int pageOrdinal = getPageOrdinal(dataPageCountReadSoFar);
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
          }
        }
        PageHeader pageHeader = readPageHeader(headerBlockDecryptor, pageHeaderAAD);
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();
        final BytesInput pageBytes;
        switch (pageHeader.type) {
          case DICTIONARY_PAGE:
            // there is only one dictionary page per column chunk
            if (dictionaryPage != null) {
              throw new ParquetDecodingException(
                  "more than one dictionary page in column " + descriptor.col);
            }
            pageBytes = this.readAsBytesInput(compressedPageSize);
            if (options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
              verifyCrc(
                  pageHeader.getCrc(),
                  pageBytes.toByteArray(),
                  "could not verify dictionary page integrity, CRC checksum verification failed");
            }
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            dictionaryPage =
                new DictionaryPage(
                    pageBytes,
                    uncompressedPageSize,
                    dicHeader.getNum_values(),
                    converter.getEncoding(dicHeader.getEncoding()));
            // Copy crc to new page, used for testing
            if (pageHeader.isSetCrc()) {
              dictionaryPage.setCrc(pageHeader.getCrc());
            }
            break;
          case DATA_PAGE:
            DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
            pageBytes = this.readAsBytesInput(compressedPageSize);
            if (options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
              verifyCrc(
                  pageHeader.getCrc(),
                  pageBytes.toByteArray(),
                  "could not verify page integrity, CRC checksum verification failed");
            }
            DataPageV1 dataPageV1 =
                new DataPageV1(
                    pageBytes,
                    dataHeaderV1.getNum_values(),
                    uncompressedPageSize,
                    converter.fromParquetStatistics(
                        getFileMetaData().getCreatedBy(), dataHeaderV1.getStatistics(), type),
                    converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                    converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                    converter.getEncoding(dataHeaderV1.getEncoding()));
            // Copy crc to new page, used for testing
            if (pageHeader.isSetCrc()) {
              dataPageV1.setCrc(pageHeader.getCrc());
            }
            pagesInChunk.add(dataPageV1);
            valuesCountReadSoFar += dataHeaderV1.getNum_values();
            ++dataPageCountReadSoFar;
            break;
          case DATA_PAGE_V2:
            DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
            int dataSize =
                compressedPageSize
                    - dataHeaderV2.getRepetition_levels_byte_length()
                    - dataHeaderV2.getDefinition_levels_byte_length();
            pagesInChunk.add(
                new DataPageV2(
                    dataHeaderV2.getNum_rows(),
                    dataHeaderV2.getNum_nulls(),
                    dataHeaderV2.getNum_values(),
                    this.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                    this.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                    converter.getEncoding(dataHeaderV2.getEncoding()),
                    this.readAsBytesInput(dataSize),
                    uncompressedPageSize,
                    converter.fromParquetStatistics(
                        getFileMetaData().getCreatedBy(), dataHeaderV2.getStatistics(), type),
                    dataHeaderV2.isIs_compressed()));
            valuesCountReadSoFar += dataHeaderV2.getNum_values();
            ++dataPageCountReadSoFar;
            break;
          default:
            LOG.debug(
                "skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
            stream.skipFully(compressedPageSize);
            break;
        }
      }
      if (offsetIndex == null && valuesCountReadSoFar != descriptor.metadata.getValueCount()) {
        // Would be nice to have a CorruptParquetFileException or something as a subclass?
        throw new IOException(
            "Expected "
                + descriptor.metadata.getValueCount()
                + " values in column chunk at "
                + getFile()
                + " offset "
                + descriptor.metadata.getFirstDataPageOffset()
                + " but got "
                + valuesCountReadSoFar
                + " values instead over "
                + pagesInChunk.size()
                + " pages ending at file offset "
                + (descriptor.fileOffset + stream.position()));
      }
      BytesInputDecompressor decompressor =
          options.getCodecFactory().getDecompressor(descriptor.metadata.getCodec());
      return new ColumnChunkPageReader(
          decompressor,
          pagesInChunk,
          dictionaryPage,
          offsetIndex,
          rowCount,
          pageBlockDecryptor,
          aadPrefix,
          rowGroupOrdinal,
          columnOrdinal);
    }

    private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar) {
      return offsetIndex == null
          ? valuesCountReadSoFar < descriptor.metadata.getValueCount()
          : dataPageCountReadSoFar < offsetIndex.getPageCount();
    }

    private int getPageOrdinal(int dataPageCountReadSoFar) {
      if (null == offsetIndex) {
        return dataPageCountReadSoFar;
      }

      return offsetIndex.getPageOrdinal(dataPageCountReadSoFar);
    }

    /**
     * @param size the size of the page
     * @return the page
     * @throws IOException if there is an error while reading from the file stream
     */
    public BytesInput readAsBytesInput(int size) throws IOException {
      return BytesInput.from(stream.sliceBuffers(size));
    }
  }

  /** deals with a now fixed bug where compressedLength was missing a few bytes. */
  private class WorkaroundChunk extends Chunk {

    private final SeekableInputStream f;

    /**
     * @param descriptor the descriptor of the chunk
     * @param f the file stream positioned at the end of this chunk
     */
    private WorkaroundChunk(
        ChunkDescriptor descriptor,
        List<ByteBuffer> buffers,
        SeekableInputStream f,
        OffsetIndex offsetIndex,
        long rowCount) {
      super(descriptor, buffers, offsetIndex, rowCount);
      this.f = f;
    }

    protected PageHeader readPageHeader() throws IOException {
      PageHeader pageHeader;
      stream.mark(8192); // headers should not be larger than 8k
      try {
        pageHeader = Util.readPageHeader(stream);
      } catch (IOException e) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        // if the last page is smaller than this, the page header itself is truncated in the buffer.
        stream.reset(); // resetting the buffer to the position before we got the error
        LOG.info("completing the column chunk to read the page header");
        pageHeader =
            Util.readPageHeader(
                new SequenceInputStream(
                    stream, f)); // trying again from the buffer + remainder of the stream.
      }
      return pageHeader;
    }

    public BytesInput readAsBytesInput(int size) throws IOException {
      int available = stream.available();
      if (size > available) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        int missingBytes = size - available;
        LOG.info("completed the column chunk with {} bytes", missingBytes);

        List<ByteBuffer> streamBuffers = stream.sliceBuffers(available);

        ByteBuffer lastBuffer = ByteBuffer.allocate(missingBytes);
        f.readFully(lastBuffer);

        List<ByteBuffer> buffers = new ArrayList<>(streamBuffers.size() + 1);
        buffers.addAll(streamBuffers);
        buffers.add(lastBuffer);

        return BytesInput.from(buffers);
      }

      return super.readAsBytesInput(size);
    }
  }

  /** Information needed to read a column chunk or a part of it. */
  private static class ChunkDescriptor {

    private final ColumnDescriptor col;
    private final ColumnChunkMetaData metadata;
    private final long fileOffset;
    private final long size;

    /**
     * @param col column this chunk is part of
     * @param metadata metadata for the column
     * @param fileOffset offset in the file where this chunk starts
     * @param size size of the chunk
     */
    private ChunkDescriptor(
        ColumnDescriptor col, ColumnChunkMetaData metadata, long fileOffset, long size) {
      super();
      this.col = col;
      this.metadata = metadata;
      this.fileOffset = fileOffset;
      this.size = size;
    }

    @Override
    public int hashCode() {
      return col.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj instanceof ChunkDescriptor) {
        return col.equals(((ChunkDescriptor) obj).col);
      } else {
        return false;
      }
    }
  }

  /**
   * Describes a list of consecutive parts to be read at once. A consecutive part may contain whole
   * column chunks or only parts of them (some pages).
   */
  private class ConsecutivePartList {

    private final long offset;
    private long length;
    private final List<ChunkDescriptor> chunks = new ArrayList<>();

    /** @param offset where the first chunk starts */
    ConsecutivePartList(long offset) {
      this.offset = offset;
    }

    /**
     * adds a chunk to the list. It must be consecutive to the previous chunk
     *
     * @param descriptor a chunk descriptor
     */
    public void addChunk(ChunkDescriptor descriptor) {
      chunks.add(descriptor);
      length += descriptor.size;
    }

    /**
     * @param f file to read the chunks from
     * @param builder used to build chunk list to read the pages for the different columns
     * @throws IOException if there is an error while reading from the stream
     */
    public void readAll(SeekableInputStream f, ChunkListBuilder builder) throws IOException {
      f.seek(offset);

      int fullAllocations = Math.toIntExact(length / options.getMaxAllocationSize());
      int lastAllocationSize = Math.toIntExact(length % options.getMaxAllocationSize());

      int numAllocations = fullAllocations + (lastAllocationSize > 0 ? 1 : 0);
      List<ByteBuffer> buffers = new ArrayList<>(numAllocations);

      for (int i = 0; i < fullAllocations; i += 1) {
        buffers.add(options.getAllocator().allocate(options.getMaxAllocationSize()));
      }

      if (lastAllocationSize > 0) {
        buffers.add(options.getAllocator().allocate(lastAllocationSize));
      }

      for (ByteBuffer buffer : buffers) {
        f.readFully(buffer);
        buffer.flip();
      }

      ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffers);
      for (final ChunkDescriptor descriptor : chunks) {
        builder.add(descriptor, stream.sliceBuffers(descriptor.size), f);
      }
    }

    /** @return the position following the last byte of these chunks */
    public long endPos() {
      return offset + length;
    }
  }
}
