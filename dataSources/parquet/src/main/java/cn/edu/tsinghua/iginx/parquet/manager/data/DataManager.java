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

package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.lsm.OneTierDB;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataManager implements Manager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataManager.class);

  private final Database<Long, String, DataType, Object> db;

  private final Shared shared;

  public DataManager(Shared shared, Path dir) throws IOException {
    this.shared = shared;
    Path dataDir = dir.resolve(Constants.DIR_NAME_TABLE);
    ReadWriter<Long, String, DataType, Object> readWriter = new ParquetReadWriter(shared, dataDir);
    this.db = new OneTierDB<>(dir.toString(), shared, readWriter);
  }

  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    Map<String, DataType> schema = ArrowFields.toIginxSchema(db.schema());
    Map<String, DataType> schemaMatchTags = ProjectUtils.project(schema, tagFilter);

    Map<String, DataType> projectedSchema = ProjectUtils.project(schemaMatchTags, paths);
    Filter projectedFilter = ProjectUtils.project(filter, schemaMatchTags);
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(projectedFilter);

    Scanner<Long, Scanner<String, Object>> scanner =
        db.query(ArrowFields.of(projectedSchema), rangeSet, projectedFilter);
    return new ScannerRowStream(projectedSchema, scanner);
  }

  @Override
  public void insert(DataView data) throws PhysicalException {
    DataViewWrapper wrappedData = new DataViewWrapper(data);
    if (wrappedData.isRowData()) {
      try (Scanner<Long, Scanner<String, Object>> scanner = wrappedData.getRowsScanner()) {
        db.upsertRows(scanner, wrappedData.getSchema());
      }
    } else {
      try (Scanner<String, Scanner<Long, Object>> scanner = wrappedData.getColumnsScanner()) {
        db.upsertColumns(scanner, wrappedData.getSchema());
      }
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
  public List<Column> getColumns() throws StorageException {
    List<Column> columns = new ArrayList<>();
    Map<String, DataType> schema = ArrowFields.toIginxSchema(db.schema());
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      columns.add(new Column(pathWithTags.getKey(), entry.getValue(), pathWithTags.getValue()));
    }
    return columns;
  }

  @Override
  public void close() throws Exception {
    db.close();
  }
}
