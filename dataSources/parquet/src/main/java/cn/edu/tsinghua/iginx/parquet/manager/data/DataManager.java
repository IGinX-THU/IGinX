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
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.common.Config;
import cn.edu.tsinghua.iginx.parquet.common.Constants;
import cn.edu.tsinghua.iginx.parquet.common.exception.InvalidFieldNameException;
import cn.edu.tsinghua.iginx.parquet.common.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.common.scanner.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.OneTierDB;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ObjectFormat;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IRecord;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetMeta;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import javax.annotation.Nonnull;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataManager implements Manager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataManager.class);

  private final Database<Long, String, DataType, Object> db;

  private final Config config;

  public DataManager(Config config, Path dir) throws IOException {
    this.config = config;
    this.db =
        new OneTierDB<>(
            config,
            dir,
            new ReadWriter<Long, String, Object, DataType>() {
              @Override
              public void flush(
                  Path path,
                  Scanner<Long, Scanner<String, Object>> scanner,
                  Map<String, DataType> schema,
                  Map<String, String> extra)
                  throws IOException {
                DataManager.this.flush(path, scanner, schema, extra);
              }

              @Override
              public Map.Entry<Map<String, DataType>, Map<String, String>> readMeta(Path path)
                  throws IOException {
                return DataManager.this.read(path);
              }

              @Override
              public Scanner<Long, Scanner<String, Object>> scanData(
                  Path path, Set<String> fields, Range<Long> range, Filter filter)
                  throws IOException {
                return DataManager.this.scan(path, fields, range, filter);
              }

              @Override
              public ObjectFormat<Long> getKeyFormat() {
                return new LongFormat();
              }

              @Override
              public ObjectFormat<String> getFieldFormat() {
                return new StringFormat();
              }
            });
  }

  private Map.Entry<Map<String, DataType>, Map<String, String>> read(Path path) throws IOException {
    try (IParquetReader reader = IParquetReader.builder(path).build()) {
      Map<String, DataType> schemaDst = new HashMap<>();
      MessageType parquetSchema = reader.getSchema();
      for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
        Type type = parquetSchema.getType(i);
        if (type.getName().equals(Constants.KEY_FIELD_NAME)) {
          continue;
        }
        DataType iginxType = ParquetMeta.toIginxType(type.asPrimitiveType());
        schemaDst.put(type.getName(), iginxType);
      }

      return new AbstractMap.SimpleImmutableEntry<>(
          schemaDst, Collections.unmodifiableMap(reader.getExtra()));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("failed to close reader of " + path, e);
    }
  }

  private void flush(
      Path path,
      Scanner<Long, Scanner<String, Object>> scanner,
      Map<String, DataType> schema,
      Map<String, String> extra)
      throws IOException {
    List<Type> fields = new ArrayList<>();
    fields.add(
        Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    schema.forEach(
        (name, type) -> {
          fields.add(Storer.getParquetType(name, type, Type.Repetition.OPTIONAL));
        });
    MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);

    IParquetWriter.Builder builder = IParquetWriter.builder(path, parquetSchema);

    builder.withRowGroupSize(config.getParquetRowGroupSize());
    builder.withPageSize((int) config.getParquetPageSize());

    for (Map.Entry<String, String> entry : extra.entrySet()) {
      builder.withExtraMetaData(entry.getKey(), entry.getValue());
    }

    Files.createDirectories(path.getParent());
    try (IParquetWriter writer = builder.build()) {
      while (scanner.iterate()) {
        IRecord record = IParquetWriter.getRecord(parquetSchema, scanner.key(), scanner.value());
        writer.write(record);
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("failed to close", e);
    }
  }

  private Scanner<Long, Scanner<String, Object>> scan(
      Path path, Set<String> fields, Range<Long> range, Filter filter) throws IOException {

    AndFilter unionFilter = FilterRangeUtils.filterOf(range);
    if (filter != null) {
      unionFilter.getChildren().add(filter);
    }

    return new Scanner<Long, Scanner<String, Object>>() {
      private final IParquetReader reader =
          IParquetReader.builder(path).project(fields).filter(unionFilter).build();
      private Long key;
      private Scanner<String, Object> rowScanner;

      @Nonnull
      @Override
      public Long key() throws NoSuchElementException {
        if (key == null) {
          throw new NoSuchElementException();
        }
        return key;
      }

      @Nonnull
      @Override
      public Scanner<String, Object> value() throws NoSuchElementException {
        if (rowScanner == null) {
          throw new NoSuchElementException();
        }
        return rowScanner;
      }

      @Override
      public boolean iterate() throws StorageException {
        try {
          IRecord record = reader.read();
          if (record == null) {
            key = null;
            rowScanner = null;
            return false;
          }
          Map<String, Object> map = new HashMap<>();
          MessageType parquetSchema = reader.getSchema();
          int keyIndex = parquetSchema.getFieldIndex(Constants.KEY_FIELD_NAME);
          for (Map.Entry<Integer, Object> entry : record) {
            int index = entry.getKey();
            Object value = entry.getValue();
            if (index == keyIndex) {
              key = (Long) value;
            } else {
              map.put(parquetSchema.getType(index).getName(), value);
            }
          }
          rowScanner = new IteratorScanner<>(map.entrySet().iterator());
          return true;
        } catch (Exception e) {
          throw new StorageException("failed to read", e);
        }
      }

      @Override
      public void close() throws StorageException {
        try {
          reader.close();
        } catch (Exception e) {
          throw new StorageException("failed to close", e);
        }
      }
    };
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
    for (String fullName : wrappedData.getSchema().keySet()) {
      if (StringUtils.isPattern(fullName)) {
        throw new InvalidFieldNameException(fullName, "name is a pattern");
      }
    }
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

    if (ProjectUtils.allMatched(paths) && tagFilter == null) {
      if (rangeSet.isEmpty()) {
        db.delete();
      } else {
        db.delete(rangeSet);
      }
    } else {
      Map<String, DataType> schemaMatchedTags = ProjectUtils.project(db.schema(), tagFilter);
      Set<String> fields = ProjectUtils.project(schemaMatchedTags, paths).keySet();
      if (rangeSet.isEmpty()) {
        db.delete(fields);
      } else {
        db.delete(fields, rangeSet);
      }
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
    if (!optionalRange.isPresent()) {
      return new KeyInterval(0, 0);
    }

    long begin = 0, end = Long.MAX_VALUE;
    Range<Long> span = optionalRange.get();
    if (span.hasLowerBound()) {
      begin = span.lowerEndpoint();
      if (span.lowerBoundType() == BoundType.OPEN) {
        begin += 1;
      }
    }
    if (span.hasUpperBound()) {
      end = span.upperEndpoint();
      if (span.upperBoundType() == BoundType.OPEN) {
        end -= 1;
      }
    }
    return new KeyInterval(begin, end);
  }

  @Override
  public void close() throws Exception {
    db.close();
  }
}
