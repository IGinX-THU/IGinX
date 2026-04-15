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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.SparseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import lombok.Value;
import org.apache.arrow.util.Preconditions;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TsfileFormat extends SparseImmutableFileFormat {

  private final TsfileConfig tsfileConfig;

  public TsfileFormat(Config config, CachePool cachePool, Indexer indexer) {
    super("tsfile", TsfileConfig.of(config), cachePool);
    this.tsfileConfig = (TsfileConfig) this.config;
  }

  @Override
  protected void flush(Path dstWithSuffix, List<Table.SubTable> subTables) throws IOException, PhysicalException {
    TSFileConfig tsFileConfig = new TSFileConfig();
    tsFileConfig.setCompressor(tsfileConfig.getCompression().name());

    try (TsFileWriter tsFileWriter = new TsFileWriter(dstWithSuffix.toFile(), new Schema(), tsFileConfig)) {
      for (int subTableIndex = 0; subTableIndex < subTables.size(); subTableIndex++) {
        String deviceId = String.format("subtable%010d", subTableIndex);
        Table.SubTable subTable = subTables.get(subTableIndex);
        try (RowStream rowStream = scanAll(subTable)) {
          Header header = rowStream.getHeader();
          List<IMeasurementSchema> schema = new ArrayList<>();
          for (Field field : header.getFields()) {
            schema.add(TypeUtils.toTsfileField(field, tsFileConfig));
          }
          tsFileWriter.registerAlignedTimeseries(deviceId, schema);
          while (rowStream.hasNext()) {
            Row row = rowStream.next();
            TSRecord record = TypeUtils.toTsRecord(deviceId, schema, row.getKey(), row.getValues());
            tsFileWriter.writeRecord(record);
          }
        }
      }
    } catch (WriteProcessException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Object loadFileMeta(Path srcWithSuffix) throws IOException {
    try (TsFileSequenceReader metaReader = new TsFileSequenceReader(srcWithSuffix.toFile().getPath())) {
      return new FileMeta(
          metaReader.getAllTimeseriesMetadata(false).entrySet().stream()
              .collect(ImmutableMap.toImmutableMap(entry -> entry.getKey().toString(), Map.Entry::getValue))
      );
    }
  }

  @Value
  private static class FileMeta {
    ImmutableMap<String, List<TimeseriesMetadata>> allTimeseriesMetadata;
  }

  @Override
  protected List<String> readSubTableNames(Path srcWithSuffix) throws IOException {
    FileMeta fileMeta = (FileMeta) getOrLoadFileMeta(srcWithSuffix);
    return ImmutableList.copyOf(fileMeta.getAllTimeseriesMetadata().keySet());
  }

  @Override
  protected Table.Meta loadMeta(Path srcWithSuffix, String subTableName) throws IOException {
    FileMeta fileMeta = (FileMeta) getOrLoadFileMeta(srcWithSuffix);
    List<TimeseriesMetadata> subTableMeta = fileMeta.getAllTimeseriesMetadata().get(subTableName);

    if (subTableMeta == null) {
      throw new IOException("Subtable " + subTableName + " not found in file " + srcWithSuffix);
    }

    ImmutableMap.Builder<Field, Table.Statistic> fieldStatsBuilder = ImmutableMap.builder();
    for (TimeseriesMetadata timeseriesMetadata : subTableMeta) {
      if (timeseriesMetadata.getMeasurementId().isEmpty()) {
        continue;
      }
      Field field = TypeUtils.toIginxField(timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTsDataType());
      Statistics<?> stat = timeseriesMetadata.getStatistics();
      Range<Long> keyRange = Range.closed(stat.getStartTime(), stat.getEndTime());
      fieldStatsBuilder.put(field, new Table.Statistic(keyRange));
    }
    return new Table.Meta(fieldStatsBuilder.build());
  }

  @Override
  protected RowStream scan(Path srcWithSuffix, String subTableName, List<Field> fields, Filter predicate) throws IOException {
    Header header = new Header(Field.KEY, fields);

    List<org.apache.tsfile.read.common.Path> tsfileFields = new ArrayList<>();
    for (Field field : fields) {
      tsfileFields.add(TypeUtils.toTsfilePath(subTableName, field));
    }

    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(predicate);
    Filter filterWithoutKeyRange = FilterRangeUtils.withoutKeyRangeSet(predicate);
    IExpression expression = FilterUtils.toTsfileExpression(rangeSet);

    QueryExpression queryExpression = QueryExpression.create(tsfileFields, expression);

    List<Row> rows = new ArrayList<>();
    try (TsFileReader tsFileReader = new TsFileReader(srcWithSuffix.toFile())) {
      QueryDataSet dataset = tsFileReader.query(queryExpression);
      List<org.apache.tsfile.read.common.Path> resultFields = dataset.getPaths();

      Preconditions.checkState(resultFields.equals(tsfileFields), "Returned fields do not match requested fields");

      while (dataset.hasNext()) {
        RowRecord record = dataset.next();
        long key = record.getTimestamp();
        List<org.apache.tsfile.read.common.Field> rowFields = record.getFields();
        Object[] values = rowFields.stream().map(TypeUtils::toIginxValue).toArray();
        rows.add(new Row(header, key, values));
      }
    }
    RowStream result = new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    if (!Filters.isTrue(filterWithoutKeyRange)) {
      result = new FilterRowStreamWrapper(result, filterWithoutKeyRange);
    }
    return result;
  }
}
