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
package org.apache.parquet.local;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.io.api.RecordMaterializer.RecordMaterializationException;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRecordReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReader.class);

  private ColumnIOFactory columnIOFactory = null;
  private final Filter filter;
  private MessageType requestedSchema;
  private MessageType fileSchema;
  private RecordMaterializer<T> recordMaterializer;
  private T currentValue;
  private long total;
  private long current = 0;
  private int currentBlock = -1;
  private ParquetFileReader reader;
  private long currentRowIdx = -1;
  private PrimitiveIterator.OfLong rowIdxInFileItr;
  private org.apache.parquet.io.RecordReader<T> recordReader;

  private long totalCountLoadedSoFar = 0;

  public ParquetRecordReader(
      RecordMaterializer<T> recordMaterializer,
      ParquetFileReader reader,
      ParquetReadOptions options) {
    this.recordMaterializer = recordMaterializer;
    this.filter =
        options.getRecordFilter() == null || !options.useRecordFilter()
            ? FilterCompat.NOOP
            : options.getRecordFilter();
    this.reader = reader;
    this.requestedSchema = reader.getRequestedSchema();
    this.total = reader.getFilteredRecordCount();

    FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
    this.fileSchema = parquetFileMetadata.getSchema();
    this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
  }

  private void checkRead() throws IOException {
    if (current == totalCountLoadedSoFar) {
      PageReadStore pages = reader.readNextFilteredRowGroup();
      if (pages == null) {
        throw new IOException(
            "expecting more rows but reached last block. Read " + current + " out of " + total);
      }
      resetRowIndexIterator(pages);

      MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema);
      recordReader = columnIO.getRecordReader(pages, recordMaterializer, filter);
      totalCountLoadedSoFar += pages.getRowCount();
      ++currentBlock;
    }
  }

  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  public T getCurrentValue() {
    return currentValue;
  }

  public boolean nextKeyValue() throws IOException {
    boolean recordFound = false;

    while (!recordFound) {
      // no more records left
      if (current >= total) {
        return false;
      }

      try {
        checkRead();
        current++;

        try {
          currentValue = recordReader.read();
          if (rowIdxInFileItr != null && rowIdxInFileItr.hasNext()) {
            currentRowIdx = rowIdxInFileItr.next();
          } else {
            currentRowIdx = -1;
          }
        } catch (RecordMaterializationException e) {
          // this might throw, but it's fatal if it does.
          LOG.debug("skipping a corrupt record");
          continue;
        }

        if (recordReader.shouldSkipCurrentRecord()) {
          // this record is being filtered via the filter2 package
          LOG.debug("skipping record");
          continue;
        }

        if (currentValue == null) {
          // only happens with FilteredRecordReader at end of block
          current = totalCountLoadedSoFar;
          LOG.debug("filtered record reader reached end of block");
          continue;
        }

        recordFound = true;

        LOG.debug("read value: {}", currentValue);
      } catch (RuntimeException e) {
        throw new ParquetDecodingException(
            format(
                "Can not read value at %d in block %d in file %s",
                current, currentBlock, reader.getFile()),
            e);
      }
    }
    return true;
  }

  /**
   * Returns the row index of the current row. If no row has been processed or if the row index
   * information is unavailable from the underlying @{@link PageReadStore}, returns -1.
   */
  public long getCurrentRowIndex() {
    if (current == 0L || rowIdxInFileItr == null) {
      return -1;
    }
    return currentRowIdx;
  }

  /** Resets the row index iterator based on the current processed row group. */
  private void resetRowIndexIterator(PageReadStore pages) {
    Optional<Long> rowGroupRowIdxOffset = pages.getRowIndexOffset();
    if (!rowGroupRowIdxOffset.isPresent()) {
      this.rowIdxInFileItr = null;
      return;
    }

    currentRowIdx = -1;
    final PrimitiveIterator.OfLong rowIdxInRowGroupItr;
    if (pages.getRowIndexes().isPresent()) {
      rowIdxInRowGroupItr = pages.getRowIndexes().get();
    } else {
      rowIdxInRowGroupItr = LongStream.range(0, pages.getRowCount()).iterator();
    }
    // Adjust the row group offset in the `rowIndexWithinRowGroupIterator` iterator.
    this.rowIdxInFileItr =
        new PrimitiveIterator.OfLong() {
          public long nextLong() {
            return rowGroupRowIdxOffset.get() + rowIdxInRowGroupItr.nextLong();
          }

          public boolean hasNext() {
            return rowIdxInRowGroupItr.hasNext();
          }

          public Long next() {
            return rowGroupRowIdxOffset.get() + rowIdxInRowGroupItr.next();
          }
        };
  }
}
