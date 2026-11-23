/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.DenseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.*;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.shade.org.apache.parquet.column.statistics.Statistics;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringBitmap32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ref: paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java

public class ParquetFormat extends DenseImmutableFileFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFormat.class);

  private final ParquetConfig parquetConfig;
  private final ParquetFileFormat format;
  private final ParquetFormatIndexer indexer;

  public ParquetFormat(Config config, CachePool cachePool, Indexer indexer) {
    super("parquet", ParquetConfig.of(config), cachePool);
    this.parquetConfig = (ParquetConfig) this.config;
    this.format =
        new ParquetFileFormat(
            new FileFormatFactory.FormatContext(
                new Options(),
                parquetConfig.readBatchSize,
                parquetConfig.writeBatchSize,
                MemorySize.ofBytes(parquetConfig.writeBatchMemory.toBytes()),
                parquetConfig.getZstdLevel(),
                null));
    this.indexer = new ParquetFormatIndexer(parquetConfig.getIndex(), cachePool, indexer, format);
  }

  @Override
  protected void flush(Path dst, Table.SubTable subTable) throws IOException, PhysicalException {
    org.apache.paimon.fs.Path paimonDst = new org.apache.paimon.fs.Path(dst.toUri());
    RowType schema;
    try (RowStream rowStream = scanAll(subTable)) {
      schema = TypeUtils.toRowType(rowStream.getHeader());
      FormatWriterFactory writerFactory = format.createWriterFactory(schema);
      try (LocalFileIO localFileIO = LocalFileIO.create();
          PositionOutputStream outputStream = localFileIO.newOutputStream(paimonDst, false);
          FormatWriter writer =
              writerFactory.create(outputStream, parquetConfig.getCompression().name())) {
        while (rowStream.hasNext()) {
          Row row = rowStream.next();
          InternalRow paimonRow = TypeUtils.toInternalRow(row);
          writer.addElement(paimonRow);
        }
      }
    }
    if (parquetConfig.getIndex().getHitCountThreshold() == 0) {
      for (DataField field : schema.getFields().subList(1, schema.getFieldCount())) {
        try {
          indexer.buildIndex(dst, field.name(), field.type());
        } catch (Throwable e) {
          LOGGER.error(
              "Failed to build index for field {} of subtable {}, skipping index building for this field",
              field.name(),
              dst.getFileName(),
              e);
        }
      }
    }
  }

  @Override
  protected Table.Meta loadMeta(Path src) throws IOException {
    org.apache.paimon.fs.Path paimonDst = new org.apache.paimon.fs.Path(src.toUri());
    try (LocalFileIO localFileIO = LocalFileIO.create()) {
      FileStatus fileStatus = localFileIO.getFileStatus(paimonDst);
      Pair<Map<String, Statistics<?>>, SimpleStatsExtractor.FileInfo> stats =
          ParquetUtil.extractColumnStats(localFileIO, paimonDst, fileStatus.getLen());
      ImmutableMap<Field, Table.Statistic> statisticMap = TypeUtils.toStatisticMap(stats.getLeft());
      return new Table.Meta(statisticMap);
    }
  }

  @Override
  protected RowStream scan(Path src, List<Field> fields, Filter predicate) throws IOException {
    org.apache.paimon.fs.Path paimonSrc = new org.apache.paimon.fs.Path(src.toUri());
    Header header = new Header(Field.KEY, fields);

    if (Filters.isFalse(predicate)) {
      return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(
          header, Collections.emptyList());
    }

    RowType projectedSchema = TypeUtils.toRowType(header);
    InternalRow.FieldGetter[] fieldGetters =
        IntStream.range(0, projectedSchema.getFieldCount())
            .mapToObj(i -> InternalRow.createFieldGetter(projectedSchema.getTypeAt(i), i))
            .toArray(InternalRow.FieldGetter[]::new);

    List<Filter> unhandledFilters = new ArrayList<>();
    Predicate paimonPredicate =
        FilterUtils.toPaimonPredicate(predicate, header, projectedSchema, unhandledFilters);

    List<Filter> postingFilters = new ArrayList<>();
    List<java.util.function.Predicate<InternalRow>> otherPredicates = new ArrayList<>();
    for (Filter unhandledFilter : unhandledFilters) {
      java.util.function.Predicate<InternalRow> otherPredicate =
          FilterUtils.optimizeUnsupportedFilter(unhandledFilter, header, fieldGetters);
      if (otherPredicate != null) {
        otherPredicates.add(otherPredicate);
      } else {
        postingFilters.add(unhandledFilter);
      }
    }

    Predicate optimizedPaimonPredicate = PredicateDeduper.simplify(paimonPredicate);
    RoaringBitmap32 selection = null;
    if (optimizedPaimonPredicate != null) {
      FileIndexResult indexResult = indexer.useIndex(src, paimonPredicate);
      if (!indexResult.remain()) {
        return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(
            header, Collections.emptyList());
      }
      if (indexResult instanceof BitmapIndexResult) {
        selection = ((BitmapIndexResult) indexResult).get();
      }
    }

    FormatReaderFactory readerFactory =
        format.createReaderFactory(
            null, projectedSchema, PredicateBuilder.splitAnd(optimizedPaimonPredicate));

    List<Row> rows = new ArrayList<>();
    long allReadRows = 0;
    long rowsFilteredByOtherPredicate = 0;
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(predicate);
    try (LocalFileIO localFileIO = LocalFileIO.create()) {
      FileStatus fileStatus = localFileIO.getFileStatus(paimonSrc);
      FormatReaderContext context =
          new FormatReaderContext(localFileIO, paimonSrc, fileStatus.getLen(), selection);
      try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
        while (true) {
          FileRecordIterator<InternalRow> paimonBatch = reader.readBatch();
          if (paimonBatch == null) {
            break;
          }
          while (true) {
            InternalRow paimonRow = paimonBatch.next();
            if (paimonRow == null) {
              break;
            }
            allReadRows++;
            int rowPosition = Math.toIntExact(paimonBatch.returnedPosition());
            boolean filtered = !testPredicate(paimonRow, rowPosition, selection, paimonPredicate);
            if (filtered) {
              long key = (Long) fieldGetters[0].getFieldOrNull(paimonRow);
              if (rangeSet.contains(key)) {
                rowsFilteredByOtherPredicate++;
              }
              continue;
            }
            if (otherPredicates.stream().anyMatch(p -> !p.test(paimonRow))) {
              continue;
            }
            Row row = TypeUtils.toRow(paimonRow, header, fieldGetters);
            rows.add(row);
          }
        }
      }
    }

    if (paimonPredicate != null && allReadRows > 0) {
      double selectivity = 1 - ((double) rowsFilteredByOtherPredicate / (double) allReadRows);
      Map<String, DataType> predicateFields = FilterUtils.extractFields(paimonPredicate);
      indexer.tryBuildIndex(src, predicateFields, selectivity);
    }

    RowStream rowStream =
        new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    for (Filter postingFilter : postingFilters) {
      if (!Filters.isTrue(postingFilter)) {
        rowStream = new FilterRowStreamWrapper(rowStream, postingFilter);
      }
    }
    return rowStream;
  }

  private static boolean testPredicate(
      InternalRow paimonRow,
      int position,
      @Nullable RoaringBitmap32 selection,
      @Nullable Predicate predicate) {
    if (selection != null && !selection.contains(position)) {
      return false;
    }
    return predicate == null || predicate.test(paimonRow);
  }
}
