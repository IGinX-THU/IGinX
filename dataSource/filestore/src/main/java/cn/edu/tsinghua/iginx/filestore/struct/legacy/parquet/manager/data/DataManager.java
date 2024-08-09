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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.Database;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.OneTierDB;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Constants;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Shared;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataManager implements Manager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataManager.class);

  private final Database db;

  private final Shared shared;

  public DataManager(Shared shared, Path dir) throws IOException {
    this.shared = shared;
    Path dataDir = dir.resolve(Constants.DIR_NAME_TABLE);
    ReadWriter readWriter = new ParquetReadWriter(shared, dataDir);
    this.db = new OneTierDB(dir.toString(), shared, readWriter);
  }

  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    Map<String, DataType> schema = ArrowFields.toIginxSchema(db.schema());
    Map<String, DataType> schemaMatchTags = ProjectUtils.project(schema, tagFilter);

    Map<String, DataType> projectedSchema = ProjectUtils.project(schemaMatchTags, paths);
    Filter projectedFilter = ProjectUtils.project(filter, schemaMatchTags);
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(projectedFilter);

    try {
      Scanner<Long, Scanner<String, Object>> scanner =
          db.query(ArrowFields.of(projectedSchema), rangeSet, projectedFilter);
      return new ScannerRowStream(projectedSchema, scanner);
    } catch (IOException e) {
      throw new StorageException(e);
    }
  }

  public RowStream aggregation(List<String> patterns, TagFilter tagFilter, List<FunctionCall> calls)
      throws PhysicalException {
    Map<String, DataType> schema = ArrowFields.toIginxSchema(db.schema());
    Map<String, DataType> schemaMatchTags = ProjectUtils.project(schema, tagFilter);
    Map<String, DataType> projectedSchema = ProjectUtils.project(schemaMatchTags, patterns);

    try {
      // TODO: just support count now
      Map<String, Long> counts = db.count(ArrowFields.of(projectedSchema));
      return new AggregatedRowStream(counts, "count");
    } catch (InterruptedException | IOException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void insert(DataView data) throws PhysicalException {
    DataViewWrapper wrappedData = new DataViewWrapper(data);
    try {
      if (wrappedData.isRowData()) {
        try (Scanner<Long, Scanner<String, Object>> scanner = wrappedData.getRowsScanner()) {
          db.upsertRows(scanner, wrappedData.getSchema());
        }
      } else {
        try (Scanner<String, Scanner<Long, Object>> scanner = wrappedData.getColumnsScanner()) {
          db.upsertColumns(scanner, wrappedData.getSchema());
        }
      }
    } catch (InterruptedException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException {

    com.google.common.collect.RangeSet<Long> rangeSet =
        com.google.common.collect.TreeRangeSet.create();
    if (keyRanges != null && !keyRanges.isEmpty()) {
      for (KeyRange range : keyRanges) {
        rangeSet.add(
            com.google.common.collect.Range.closed(
                range.getActualBeginKey(), range.getActualEndKey()));
      }
    }

    AreaSet<Long, String> areas = new AreaSet<>();
    Map<String, DataType> schema = Collections.emptyMap();
    if (paths.stream().anyMatch("*"::equals) && tagFilter == null) {
      if (rangeSet.isEmpty()) {
        db.clear();
      } else {
        areas.add(rangeSet);
      }
    } else {
      schema = ArrowFields.toIginxSchema(db.schema());
      Map<String, DataType> schemaMatchedTags = ProjectUtils.project(schema, tagFilter);
      Set<String> fields = ProjectUtils.project(schemaMatchedTags, paths).keySet();
      if (rangeSet.isEmpty()) {
        areas.add(fields);
      } else {
        areas.add(fields, rangeSet);
      }
    }

    if (!areas.isEmpty()) {
      AreaSet<Long, Field> arrowAreas = ArrowFields.of(areas, schema);
      db.delete(arrowAreas);
    }
  }

  @Override
  public List<Column> getColumns(List<String> paths, TagFilter tagFilter) throws StorageException {
    List<Column> columns = new ArrayList<>();
    Map<String, DataType> schema = ArrowFields.toIginxSchema(db.schema());
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      DataType dataType = entry.getValue();
      ColumnKey columnKey = new ColumnKey(pathWithTags.getKey(), pathWithTags.getValue());
      if (!TagKVUtils.match(columnKey, paths, tagFilter)) {
        continue;
      }
      columns.add(new Column(columnKey.getPath(), dataType, columnKey.getTags()));
    }
    return columns;
  }

  @Override
  public void close() throws Exception {
    db.close();
  }
}
