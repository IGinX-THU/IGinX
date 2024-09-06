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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.format.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.filestore.format.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.filestore.format.parquet.IRecord;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.table.DeletedTableMeta;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.AreaFilterScanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.IteratorScanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.CachePool;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Constants;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Shared;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ColumnPath;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class ParquetReadWriter implements ReadWriter {

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
  public String getName() {
    return dir.toString();
  }

  @Override
  public void flush(
      String tableName, TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner)
      throws IOException {
    Path path = getPath(tableName);
    Path tempPath = dir.resolve(tableName + Constants.SUFFIX_FILE_TEMP);
    Files.createDirectories(path.getParent());

    LOGGER.debug("flushing into {}", tempPath);

    MessageType parquetSchema = getMessageType(meta.getSchema());
    int maxBufferSize = shared.getStorageProperties().getParquetOutputBufferMaxSize();
    IParquetWriter.Builder builder = IParquetWriter.builder(tempPath, parquetSchema, maxBufferSize);
    builder.withRowGroupSize(shared.getStorageProperties().getParquetRowGroupSize());
    builder.withPageSize((int) shared.getStorageProperties().getParquetPageSize());
    builder.withCompressionCodec(shared.getStorageProperties().getParquetCompression());

    try (IParquetWriter writer = builder.build()) {
      while (scanner.iterate()) {
        IRecord record = IParquetWriter.getRecord(parquetSchema, scanner.key(), scanner.value());
        writer.write(record);
      }
      ParquetMetadata parquetMeta = writer.flush();
      ParquetTableMeta tableMeta = ParquetTableMeta.of(parquetMeta);
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
  public TableMeta readMeta(String tableName) {
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

  private ParquetTableMeta getParquetTableMeta(String fileName) {
    CachePool.Cacheable cacheable =
        shared.getCachePool().asMap().computeIfAbsent(fileName, this::doReadMeta);
    if (!(cacheable instanceof TableMeta)) {
      throw new StorageRuntimeException("invalid cacheable type: " + cacheable.getClass());
    }
    return (ParquetTableMeta) cacheable;
  }

  private ParquetTableMeta doReadMeta(String fileName) {
    Path path = Paths.get(fileName);

    try (IParquetReader reader = IParquetReader.builder(path).build()) {
      ParquetMetadata meta = reader.getMeta();
      return ParquetTableMeta.of(meta);
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
  public Iterable<String> reload() throws IOException {
    List<String> names = new ArrayList<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_PARQUET)) {
      for (Path path : stream) {
        shared.getCachePool().asMap().remove(path.toString());
        String fileName = path.getFileName().toString();
        String tableName = getTableName(fileName);
        names.add(tableName);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("dir {} not existed.", dir);
    }
    tombstoneStorage.reload();
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
      try (DirectoryStream<Path> stream =
          Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_TEMP)) {
        for (Path path : stream) {
          Files.deleteIfExists(path);
        }
      }
      tombstoneStorage.clear();
      Files.deleteIfExists(dir);
    } catch (NoSuchFileException e) {
      LOGGER.trace("Not a directory to clear: {}", dir);
    } catch (DirectoryNotEmptyException e) {
      LOGGER.warn("directory not empty to clear: {}", dir);
    }
  }

  private static class ParquetScanner implements Scanner<Long, Scanner<String, Object>> {
    private final IParquetReader reader;
    private Long key;
    private Scanner<String, Object> rowScanner;

    public ParquetScanner(IParquetReader reader) {
      this.reader = reader;
    }

    @Override
    public Long key() throws NoSuchElementException {
      if (key == null) {
        throw new NoSuchElementException();
      }
      return key;
    }

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

  private static class ParquetTableMeta implements TableMeta, CachePool.Cacheable {
    private final Map<String, DataType> schemaDst;
    private final Map<String, Range<Long>> rangeMap;
    private final Map<String, Long> countMap;
    private final ParquetMetadata meta;

    public static ParquetTableMeta of(ParquetMetadata meta) {
      Map<String, DataType> schemaDst = new HashMap<>();
      Map<String, Range<Long>> rangeMap = new HashMap<>();
      Map<String, Long> countMap = new HashMap<>();
      MessageType parquetSchema = meta.getFileMetaData().getSchema();

      Range<Long> ranges = IParquetReader.getRangeOf(meta);

      for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
        Type type = parquetSchema.getType(i);
        if (type.getName().equals(Constants.KEY_FIELD_NAME)) {
          continue;
        }
        DataType iginxType = IParquetReader.toIginxType(type.asPrimitiveType());
        schemaDst.put(type.getName(), iginxType);
        rangeMap.put(type.getName(), ranges);
      }

      Map<ColumnPath, Long> columnPathMap = IParquetReader.getCountsOf(meta);
      columnPathMap.forEach(
          (columnPath, count) -> {
            String[] columnPathArray = columnPath.toArray();
            if (columnPathArray.length != 1) {
              throw new IllegalStateException("invalid column path: " + columnPath);
            }
            String name = columnPath.toArray()[0];
            countMap.put(name, count);
          });

      return new ParquetTableMeta(schemaDst, rangeMap, countMap, meta);
    }

    ParquetTableMeta(
        Map<String, DataType> schemaDst,
        Map<String, Range<Long>> rangeMap,
        Map<String, Long> countMap,
        ParquetMetadata meta) {
      this.schemaDst = schemaDst;
      this.rangeMap = rangeMap;
      this.countMap = countMap;
      this.meta = meta;
    }

    @Override
    public Map<String, DataType> getSchema() {
      return schemaDst;
    }

    @Override
    public Range<Long> getRange(String field) {
      if (!schemaDst.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return rangeMap.get(field);
    }

    @Nullable
    @Override
    public Long getValueCount(String field) {
      if (!schemaDst.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return countMap.get(field);
    }

    public ParquetMetadata getMeta() {
      return meta;
    }
  }
}
