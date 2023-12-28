package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.db.*;
import cn.edu.tsinghua.iginx.parquet.entity.*;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import cn.edu.tsinghua.iginx.parquet.io.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IRecord;
import cn.edu.tsinghua.iginx.parquet.io.parquet.Storer;
import cn.edu.tsinghua.iginx.parquet.tools.FilterRangeUtils;
import cn.edu.tsinghua.iginx.parquet.tools.ProjectUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataManager implements Manager {
  private static final Logger logger = LoggerFactory.getLogger(DataManager.class);

  private final ConcurrentHashMap<String, Column> schema = new ConcurrentHashMap<>();

  private final Database<Long, String, Object> db;

  public DataManager(Path dir) throws PhysicalException, IOException {
    this.db =
        new OneTierDB<>(
            dir,
            new ReadWriter<Long, String, Object>() {
              @Override
              public void flush(Path path, DataBuffer<Long, String, Object> buffer)
                  throws NativeStorageException {
                DataManager.this.flush(path, buffer);
              }

              @Override
              public Scanner<Long, Scanner<String, Object>> read(
                  Path path, Set<String> fields, Range<Long> range) throws NativeStorageException {
                return DataManager.this.read(path, fields, range);
              }
            });
  }

  private void flush(Path path, DataBuffer<Long, String, Object> buffer)
      throws NativeStorageException {
    List<Type> fields = new ArrayList<>();
    fields.add(
        Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    schema.forEach(
        (name, column) -> {
          fields.add(Storer.getParquetType(name, column.getDataType(), Type.Repetition.OPTIONAL));
        });
    MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);

    try (Scanner<Long, Scanner<String, Object>> scanner =
        buffer.scanRows(buffer.fields(), buffer.range())) {
      Files.createDirectories(path.getParent());
      try (IParquetWriter writer = IParquetWriter.builder(path, parquetSchema).build()) {
        while (scanner.iterate()) {
          IRecord record = IParquetWriter.getRecord(parquetSchema, scanner.key(), scanner.value());
          writer.write(record);
        }
      }
    } catch (Exception e) {
      throw new NativeStorageException("failed to flush", e);
    }
  }

  private Scanner<Long, Scanner<String, Object>> read(
      Path path, Set<String> fields, Range<Long> range) throws NativeStorageException {
    try {
      Filter filter = FilterRangeUtils.filterOf(range);
      IParquetReader reader = IParquetReader.builder(path).project(fields).filter(filter).build();
      MessageType parquetSchema = reader.getSchema();
      int keyIndex = parquetSchema.getFieldIndex(Constants.KEY_FIELD_NAME);
      return new Scanner<Long, Scanner<String, Object>>() {
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
        public boolean iterate() throws NativeStorageException {
          try {
            IRecord record = reader.read();
            if (record == null) {
              key = null;
              rowScanner = null;
              return false;
            }
            Map<String, Object> map = new HashMap<>();
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
            throw new NativeStorageException("failed to read", e);
          }
        }

        @Override
        public void close() throws NativeStorageException {
          try {
            reader.close();
          } catch (Exception e) {
            throw new NativeStorageException("failed to close", e);
          }
        }
      };
    } catch (Exception e) {
      throw new NativeStorageException("failed to read", e);
    }
  }

  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    try {
      Set<String> fieldSetMatchTags = ProjectUtils.project(schema.keySet(), tagFilter);

      Map<String, Column> projected = new HashMap<>();
      for (String field : ProjectUtils.project(fieldSetMatchTags, paths)) {
        Column column = schema.get(field);
        if (column != null) {
          projected.put(field, column);
        }
      }
      Filter projectedFilter = ProjectUtils.project(filter, fieldSetMatchTags);
      RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(projectedFilter);
      List<Scanner<Long, Scanner<String, Object>>> scanners = new ArrayList<>();
      for (Range<Long> range : rangeSet) {
        scanners.add(db.query(projected.keySet(), range));
      }
      return new ScannerRowStream(projected, new ConcatScanner<>(scanners.iterator()));
    } catch (Exception e) {
      String message =
          String.format(
              "failed to projected, details: %s, %s, %s, %s",
              paths, tagFilter, filter, schema.keySet());
      throw new PhysicalException(message, e);
    }
  }

  @Override
  public void insert(DataView data) throws PhysicalException {
    try {
      try (Scanner<String, Column> schemaScanner = new DataViewSchemaScanner(data)) {
        while (schemaScanner.iterate()) {
          String name = schemaScanner.key();
          Column column = schemaScanner.value();
          schema.putIfAbsent(name, column);
          Column curr = schema.get(name);
          if (!curr.equals(column)) {
            throw new PhysicalException(String.format("conflict data type %s->%s", column, curr));
          }
        }
      }
      if (data.isRowData()) {
        try (Scanner<Long, Scanner<String, Object>> scanner = new DataViewRowsScanner(data)) {
          db.upsertRows(scanner);
        }
      } else if (data.isColumnData()) {
        try (Scanner<String, Scanner<Long, Object>> scanner = new DataViewColumnsScanner(data)) {
          db.upsertColumns(scanner);
        }
      } else {
        throw new IllegalArgumentException("unsupported data type");
      }
    } catch (Exception e) {
      String message =
          String.format(
              "failed to insert, details: %s, %s,%s,%s",
              data.getPaths(), data.getDataTypeList(), data.getTagsList(), data.getKeySize());
      throw new PhysicalException(message, e);
    }
  }

  @Override
  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException {
    boolean allPath = ProjectUtils.isMatchAll(paths);
    Set<String> fields = new HashSet<>();
    if (!allPath) {
      fields = ProjectUtils.project(schema.keySet(), paths, tagFilter);
    }
    boolean allRange = keyRanges == null || keyRanges.isEmpty();
    RangeSet<Long> rangeSet = new RangeSet<>();
    if (!allRange) {
      for (KeyRange range : keyRanges) {
        rangeSet.add(new Range<>(range.getActualBeginKey(), range.getActualEndKey()));
      }
    }
    try {
      if (allPath && allRange) {
        db.clear();
        schema.clear();
      } else if (allPath) {
        db.deleteColumns(rangeSet);
      } else if (allRange) {
        db.deleteRows(fields);
        for (String field : fields) {
          schema.remove(field);
        }
      } else {
        db.delete(fields, rangeSet);
      }
    } catch (Exception e) {
      String message =
          String.format("failed to delete, details: %s, %s, %s", paths, keyRanges, tagFilter);
      throw new PhysicalException(message, e);
    }
  }

  @Override
  public List<Column> getColumns() {
    // TODO: recovery from disk
    return new ArrayList<>(schema.values());
  }

  @Override
  public KeyInterval getKeyInterval() {
    // TODO: recovery from disk
    return new KeyInterval(0, 0);
  }

  @Override
  public void close() throws Exception {
    db.close();
    // TODO: save to disk
  }
}
