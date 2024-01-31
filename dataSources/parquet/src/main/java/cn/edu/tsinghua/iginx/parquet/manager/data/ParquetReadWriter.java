package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ObjectFormat;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.iterator.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IRecord;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetMeta;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.parquet.shared.Constants;
import cn.edu.tsinghua.iginx.parquet.shared.Shared;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class ParquetReadWriter implements ReadWriter<Long, String, DataType, Object> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReadWriter.class);

  private final Shared shared;

  private final Path dir;

  public ParquetReadWriter(Shared shared, Path dir) {
    this.shared = shared;
    this.dir = dir;
    cleanTempFiles();
  }

  private void cleanTempFiles() {
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(dir, path -> path.endsWith(Constants.SUFFIX_FILE_TEMP))) {
      for (Path path : stream) {
        LOGGER.info("remove temp file {}", path);
        Files.deleteIfExists(path);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("no dir named {}", dir);
    } catch (IOException e) {
      LOGGER.error("failed to clean temp files", e);
    }
  }

  @Override
  public void flush(
      String tableName, Scanner<Long, Scanner<String, Object>> scanner, Map<String, DataType> types)
      throws IOException {
    Path path = getPath(tableName);
    Path tempPath = dir.resolve(tableName + Constants.SUFFIX_FILE_TEMP);

    List<Type> fields = new ArrayList<>();
    fields.add(
        Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    types.forEach(
        (name, type) -> {
          fields.add(Storer.getParquetType(name, type, Type.Repetition.OPTIONAL));
        });
    MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);

    IParquetWriter.Builder builder = IParquetWriter.builder(tempPath, parquetSchema);

    builder.withRowGroupSize(shared.getStorageProperties().getParquetRowGroupSize());
    builder.withPageSize((int) shared.getStorageProperties().getParquetPageSize());

    LOGGER.debug("flushing into {}", tempPath);
    Files.createDirectories(path.getParent());
    try (IParquetWriter writer = builder.build()) {
      while (scanner.iterate()) {
        IRecord record = IParquetWriter.getRecord(parquetSchema, scanner.key(), scanner.value());
        writer.write(record);
      }
    } catch (Exception e) {
      throw new IOException("failed to write " + path, e);
    }

    LOGGER.info("rename temp file to {}", path);
    if (Files.exists(path)) {
      LOGGER.warn("file {} already exists, will be replaced", path);
    }
    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public TableMeta<Long, String, DataType, Object> readMeta(String tableName) throws IOException {
    Path path = getPath(tableName);

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

      Range<Long> ranges = reader.getRange();

      Map<String, Range<Long>> rangeMap = new HashMap<>();
      for (String field : schemaDst.keySet()) {
        rangeMap.put(field, ranges);
      }

      return new ParquetTableMeta(schemaDst, rangeMap);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("failed to close reader of " + path, e);
    }
  }

  @Override
  public Scanner<Long, Scanner<String, Object>> scanData(
      String name, Set<String> fields, RangeSet<Long> ranges, Filter predicate) throws IOException {
    Path path = getPath(name);

    Filter rangeFilter = FilterRangeUtils.filterOf(ranges);

    Filter unionFilter;
    if (predicate == null) {
      unionFilter = rangeFilter;
    } else {
      unionFilter = new AndFilter(Arrays.asList(rangeFilter, predicate));
    }

    return new ParquetScanner(path, fields, unionFilter);
  }

  private Path getPath(String name) {
    Path path = dir.resolve(name + Constants.SUFFIX_FILE_PARQUET);
    return path;
  }

  @Override
  public Iterable<String> tableNames() throws IOException {
    List<String> names = new ArrayList<>();
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_PARQUET)) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        String tableName =
            fileName.substring(0, fileName.length() - Constants.SUFFIX_FILE_PARQUET.length());
        names.add(tableName);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.info("no dir named {}", dir);
    }
    return names;
  }

  @Override
  public void clear() throws IOException {
    LOGGER.info("clearing data of {}", dir);
    try {
      MoreFiles.deleteDirectoryContents(dir, RecursiveDeleteOption.ALLOW_INSECURE);
      Files.deleteIfExists(dir);
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("delete not existed dir named {} ", dir, ignored);
    }
  }

  @Override
  public ObjectFormat<Long> getKeyFormat() {
    return new LongFormat();
  }

  @Override
  public ObjectFormat<String> getFieldFormat() {
    return new StringFormat();
  }

  private static class ParquetScanner implements Scanner<Long, Scanner<String, Object>> {
    private final IParquetReader reader;
    private Long key;
    private Scanner<String, Object> rowScanner;

    public ParquetScanner(Path path, Set<String> fields, Filter unionFilter) throws IOException {
      reader = IParquetReader.builder(path).project(fields).filter(unionFilter).build();
    }

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
  }

  private static class ParquetTableMeta implements TableMeta<Long, String, DataType, Object> {
    private final Map<String, DataType> schemaDst;
    private final Map<String, Range<Long>> rangeMap;

    public ParquetTableMeta(Map<String, DataType> schemaDst, Map<String, Range<Long>> rangeMap) {
      this.schemaDst = schemaDst;
      this.rangeMap = rangeMap;
    }

    @Override
    public Map<String, DataType> getSchema() {
      return schemaDst;
    }

    @Override
    public Map<String, Range<Long>> getRanges() {
      return rangeMap;
    }
  }
}
