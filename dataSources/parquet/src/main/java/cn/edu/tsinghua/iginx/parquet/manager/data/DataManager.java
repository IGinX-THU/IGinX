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
package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.lsm.OneTierDB;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.manager.utils.RangeUtils;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
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
    Map<String, DataType> schemaMatchTags = ProjectUtils.project(db.schema(), tagFilter);

    Map<String, DataType> projectedSchema = ProjectUtils.project(schemaMatchTags, paths);
    Filter projectedFilter = ProjectUtils.project(filter, schemaMatchTags);
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(projectedFilter);

    Scanner<Long, Scanner<String, Object>> scanner =
        db.query(projectedSchema.keySet(), rangeSet, projectedFilter);
    return new ScannerRowStream(projectedSchema, scanner);
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
    } catch (Exception e) {
      throw new RuntimeException("failed to close scanner of DataView", e);
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
    if (paths.stream().anyMatch("*"::equals) && tagFilter == null) {
      if (rangeSet.isEmpty()) {
        db.clear();
      } else {
        areas.add(rangeSet);
      }
    } else {
      Map<String, DataType> schemaMatchedTags = ProjectUtils.project(db.schema(), tagFilter);
      Set<String> fields = ProjectUtils.project(schemaMatchedTags, paths).keySet();
      if (rangeSet.isEmpty()) {
        areas.add(fields);
      } else {
        areas.add(fields, rangeSet);
      }
    }

    if (!areas.isEmpty()) {
      db.delete(areas);
    }
  }

  @Override
  public List<Column> getColumns() throws StorageException {
    List<Column> columns = new ArrayList<>();
    for (Map.Entry<String, DataType> entry : db.schema().entrySet()) {
      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      columns.add(new Column(pathWithTags.getKey(), entry.getValue(), pathWithTags.getValue()));
    }
    return columns;
  }

  @Override
  public KeyInterval getKeyInterval() throws PhysicalException {
    Optional<Range<Long>> optionalRange = db.range();
    return optionalRange.map(RangeUtils::toKeyInterval).orElseGet(() -> new KeyInterval(0, 0));
  }

  @Override
  public void close() throws Exception {
    db.close();
  }
}
