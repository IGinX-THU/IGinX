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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.DeletedTableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.AreaFilterScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IRecord;
import cn.edu.tsinghua.iginx.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.parquet.util.CachePool;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import javax.annotation.Nonnull;
import org.ehcache.sizeof.SizeOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class ParquetReadWriter implements ReadWriter<Long, String, DataType, Object> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReadWriter.class);

  private final Shared shared;

  private final Path dir;

  private final TombstoneStorage tombstoneStorage;

  public ParquetReadWriter(Shared shared, Path dir) {
    this.shared = shared;
    this.dir = dir;
    this.tombstoneStorage = new TombstoneStorage(shared, dir.resolve(Constants.DIR_NAME_TOMBSTONE));
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
      String tableName,
      TableMeta<Long, String, DataType, Object> meta,
      Scanner<Long, Scanner<String, Object>> scanner)
      throws IOException {
    Path path = getPath(tableName);
    Path tempPath = dir.resolve(tableName + Constants.SUFFIX_FILE_TEMP);
    Files.createDirectories(path.getParent());

    LOGGER.debug("flushing into {}", tempPath);

    MessageType parquetSchema = getMessageType(meta.getSchema());
    IParquetWriter.Builder builder = IParquetWriter.builder(tempPath, parquetSchema);
    builder.withRowGroupSize(shared.getStorageProperties().getParquetRowGroupSize());
    builder.withPageSize((int) shared.getStorageProperties().getParquetPageSize());

    try (IParquetWriter writer = builder.build()) {
      while (scanner.iterate()) {
        IRecord record = IParquetWriter.getRecord(parquetSchema, scanner.key(), scanner.value());
        writer.write(record);
      }
      ParquetMetadata parquetMeta = writer.flush();
      ParquetTableMeta tableMeta =
          new ParquetTableMeta(meta.getSchema(), meta.getRanges(), parquetMeta);
      setParquetTableMeta(path.toString(), tableMeta);
    } catch (Exception e) {
      throw new IOException("failed to write " + path, e);
    }

    LOGGER.debug("rename temp file to {}", path);
    if (Files.exists(path)) {
      LOGGER.warn("file {} already exists, will be replaced", path);
    }
    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
  }

  @Nonnull
  private static MessageType getMessageType(Map<String, DataType> schema) {
    List<Type> fields = new ArrayList<>();
    fields.add(
        Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      String name = entry.getKey();
      DataType type = entry.getValue();
      fields.add(Storer.getParquetType(name, type, Type.Repetition.OPTIONAL));
    }
    MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);
    return parquetSchema;
  }

  @Override
  public TableMeta<Long, String, DataType, Object> readMeta(String tableName) {
    Path path = getPath(tableName);
    ParquetTableMeta tableMeta = getParquetTableMeta(path.toString());
    AreaSet<Long, String> tombstone = tombstoneStorage.get(tableName);
    if (tombstone == null || tombstone.isEmpty()) {
      return tableMeta;
    }
    return new DeletedTableMeta(tableMeta, tombstone);
  }

  private void setParquetTableMeta(String fileName, ParquetTableMeta tableMeta) {
    shared.getCachePool().asMap().put(fileName, tableMeta);
  }

  @Nonnull
  private ParquetTableMeta getParquetTableMeta(String fileName) {
    CachePool.Cacheable cacheable =
        shared.getCachePool().asMap().computeIfAbsent(fileName, this::doReadMeta);
    if (!(cacheable instanceof TableMeta)) {
      throw new StorageRuntimeException("invalid cacheable type: " + cacheable.getClass());
    }
    return (ParquetTableMeta) cacheable;
  }

  @Nonnull
  private ParquetTableMeta doReadMeta(String fileName) {
    Path path = Paths.get(fileName);

    try (IParquetReader reader = IParquetReader.builder(path).build()) {
      Map<String, DataType> schemaDst = new HashMap<>();
      MessageType parquetSchema = reader.getSchema();
      for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
        Type type = parquetSchema.getType(i);
        if (type.getName().equals(Constants.KEY_FIELD_NAME)) {
          continue;
        }
        DataType iginxType = IParquetReader.toIginxType(type.asPrimitiveType());
        schemaDst.put(type.getName(), iginxType);
      }

      Range<Long> ranges = reader.getRange();

      Map<String, Range<Long>> rangeMap = new HashMap<>();
      for (String field : schemaDst.keySet()) {
        rangeMap.put(field, ranges);
      }

      ParquetMetadata meta = reader.getMeta();

      return new ParquetTableMeta(schemaDst, rangeMap, meta);
    } catch (Exception e) {
      throw new StorageRuntimeException(e);
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

    IParquetReader.Builder builder = IParquetReader.builder(path);
    builder.project(fields);
    builder.filter(unionFilter);

    ParquetTableMeta parquetTableMeta = getParquetTableMeta(path.toString());
    IParquetReader reader = builder.build(parquetTableMeta.getMeta());

    Scanner<Long, Scanner<String, Object>> scanner = new ParquetScanner(reader);

    AreaSet<Long, String> tombstone = tombstoneStorage.get(name);
    if (tombstone == null || tombstone.isEmpty()) {
      return scanner;
    }
    return new AreaFilterScanner<>(scanner, tombstone);
  }

  @Override
  public void delete(String name, AreaSet<Long, String> areas) throws IOException {
    tombstoneStorage.delete(Collections.singleton(name), oldAreas -> oldAreas.addAll(areas));
  }

  @Override
  public void delete(String name) {
    Path path = getPath(name);
    try {
      Files.deleteIfExists(path);
      shared.getCachePool().asMap().remove(path.toString());
      tombstoneStorage.removeTable(name);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  public Iterable<String> tableNames() throws IOException {
    List<String> names = new ArrayList<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_PARQUET)) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        String tableName = getTableName(fileName);
        names.add(tableName);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("dir {} not existed.", dir);
    }
    return names;
  }

  private Path getPath(String name) {
    Path path = dir.resolve(name + Constants.SUFFIX_FILE_PARQUET);
    return path;
  }

  private static String getTableName(String fileName) {
    return fileName.substring(0, fileName.length() - Constants.SUFFIX_FILE_PARQUET.length());
  }

  @Override
  public void clear() throws IOException {
    LOGGER.info("clearing data of {}", dir);
    try {
      try (DirectoryStream<Path> stream =
          Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_PARQUET)) {
        for (Path path : stream) {
          Files.deleteIfExists(path);
          String fileName = path.toString();
          shared.getCachePool().asMap().remove(fileName);
        }
      }
      tombstoneStorage.clear();
      Files.deleteIfExists(dir);
    } catch (NoSuchFileException e) {
      LOGGER.trace("Not a directory to clear: {}", dir);
    } catch (DirectoryNotEmptyException e) {
      LOGGER.warn("directory not empty to clear: {}", dir);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  private static class ParquetScanner implements Scanner<Long, Scanner<String, Object>> {
    private final IParquetReader reader;
    private Long key;
    private Scanner<String, Object> rowScanner;

    public ParquetScanner(IParquetReader reader) {
      this.reader = reader;
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

  private static class ParquetTableMeta
      implements TableMeta<Long, String, DataType, Object>, CachePool.Cacheable {
    private final Map<String, DataType> schemaDst;
    private final Map<String, Range<Long>> rangeMap;
    private final ParquetMetadata meta;
    private final int weight;

    public ParquetTableMeta(
        Map<String, DataType> schemaDst, Map<String, Range<Long>> rangeMap, ParquetMetadata meta) {
      this.schemaDst = schemaDst;
      this.rangeMap = rangeMap;
      this.meta = meta;
      int schemaWeight = schemaDst.toString().length();
      int rangeWeight = rangeMap.toString().length();
      int metaWeight = (int) SizeOf.newInstance().deepSizeOf(meta);
      this.weight = schemaWeight + rangeWeight + metaWeight;
    }

    @Override
    public Map<String, DataType> getSchema() {
      return schemaDst;
    }

    @Override
    public Map<String, Range<Long>> getRanges() {
      return rangeMap;
    }

    public ParquetMetadata getMeta() {
      return meta;
    }

    @Override
    public int getWeight() {
      return weight;
    }
  }
}
