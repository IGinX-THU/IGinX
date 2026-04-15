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

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.AtomFlushPathWrapper;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.common.io.MoreFiles;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.util.AutoCloseables;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.*;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndexFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.*;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFormatIndexer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFormatIndexer.class);

  static {
    // 使用反射强行获取并注册 Factory
    try {
      Field factoriesField = FileIndexerFactoryUtils.class.getDeclaredField("factories");
      factoriesField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, FileIndexerFactory> factories =
          (Map<String, FileIndexerFactory>) factoriesField.get(null);

      // 手动注册所需的 Index Factories
      FileIndexerFactory bitmapFactory = new BitmapFileIndexFactory();
      factories.put(bitmapFactory.identifier(), bitmapFactory);

      FileIndexerFactory rangeBitmapFactory = new RangeBitmapFileIndexFactory();
      factories.put(rangeBitmapFactory.identifier(), rangeBitmapFactory);

      LOGGER.info(
          "Successfully forced registration of FileIndexerFactories: {}, {}",
          bitmapFactory.identifier(),
          rangeBitmapFactory.identifier());
    } catch (Exception e) {
      LOGGER.error("Failed to reflectively register factories in FileIndexerFactoryUtils", e);
    }
  }

  private final ParquetFileFormat format;
  private final IndexConfig config;
  private final CachePool cachePool;
  private final Indexer indexer;
  private final Table<Path, String, Integer> fieldCounter =
      Tables.newCustomTable(new HashMap<>(), Object2IntOpenHashMap::new);

  public ParquetFormatIndexer(
      IndexConfig config, CachePool cachePool, Indexer indexer, ParquetFileFormat format) {
    this.format = format;
    this.config = config;
    this.cachePool = cachePool;
    this.indexer = indexer;
  }

  public synchronized void tryBuildIndex(
      Path src, Map<String, DataType> fields, double selectivity) {
    LOGGER.debug(
        "Try to build index for file {}, fields: {}, selectivity: {}", src, fields, selectivity);
    if (selectivity > config.getSelectivityThreshold()) {
      return;
    }
    Set<String> readyFields = new HashSet<>();
    for (String field : fields.keySet()) {
      int count = Optional.ofNullable(fieldCounter.get(src, field)).orElse(0) + 1;
      fieldCounter.put(src, field, count);
      if (count == config.getHitCountThreshold()) {
        readyFields.add(field);
      }
    }
    for (String field : readyFields) {
      DataType type = fields.get(field);
      indexer.submit(
          () -> {
            try {
              buildIndex(src, field, type);
            } catch (Throwable e) {
              LOGGER.error(
                  "Failed to build index for file {}, field {}, type {}", src, field, type, e);
            }
          });
    }
  }

  public void buildIndex(Path src, String field, DataType type) throws IOException {
    LOGGER.debug("Building index for file {}, field: {}, type: {}", src, field, type);

    Map<String, byte[]> indexData = generateIndexData(src, field, type);

    Path indexFile = getIndexFile(src, field);
    MoreFiles.createParentDirectories(indexFile);
    try (AtomFlushPathWrapper wrapper = new AtomFlushPathWrapper(indexFile, true)) {
      try (OutputStream outputStream =
              Files.newOutputStream(
                  wrapper.getTmpPath(),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING);
          FileIndexFormat.Writer writer = FileIndexFormat.createWriter(outputStream)) {
        writer.writeColumnIndexes(Collections.singletonMap(field, indexData));
      }
      wrapper.commit();
    }
  }

  private Map<String, byte[]> generateIndexData(Path src, String field, DataType type)
      throws IOException {
    List<Object> values = loadAllValues(src, field, type);

    List<FileIndexerFactory> indexerFactories = getIndexers(values);
    Map<String, byte[]> indexData = new HashMap<>();
    for (FileIndexerFactory indexerFactory : indexerFactories) {
      FileIndexer indexer = indexerFactory.create(type, new Options());
      FileIndexWriter writer = indexer.createWriter();
      for (Object value : values) {
        writer.writeRecord(value);
      }
      indexData.put(indexerFactory.identifier(), writer.serializedBytes());
    }
    return indexData;
  }

  private List<FileIndexerFactory> getIndexers(List<Object> values) {
    List<FileIndexerFactory> indexers = new ArrayList<>();
    long cardinality = values.stream().filter(Objects::nonNull).distinct().count();
    if (cardinality < config.getBitmapCardinalityThreshold()) {
      indexers.add(new BitmapFileIndexFactory());
    }
    indexers.add(new RangeBitmapFileIndexFactory());
    return indexers;
  }

  private List<Object> loadAllValues(Path src, String field, DataType type) throws IOException {
    org.apache.paimon.fs.Path paimonSrc = new org.apache.paimon.fs.Path(src.toUri());
    RowType schema = RowType.of(new DataField(0, field, type));
    FormatReaderFactory readerFactory = format.createReaderFactory(null, schema, null);

    List<Object> values = new ArrayList<>();
    InternalRow.FieldGetter fieldGetter = InternalRow.createFieldGetter(schema.getTypeAt(0), 0);

    try (LocalFileIO localFileIO = LocalFileIO.create()) {
      FileStatus fileStatus = localFileIO.getFileStatus(paimonSrc);
      FormatReaderContext context =
          new FormatReaderContext(localFileIO, paimonSrc, fileStatus.getLen(), null);
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
            Object value = fieldGetter.getFieldOrNull(paimonRow);
            values.add(value);
          }
        }
      }
    }
    return values;
  }

  private static Path getIndexDir(Path parquetFile) {
    return parquetFile.resolveSibling(parquetFile.getFileName().toString() + ".index");
  }

  private static Path getIndexFile(Path parquetFile, String field) {
    String illegalUniqueKey =
        Base64.getUrlEncoder().encodeToString(field.getBytes(StandardCharsets.UTF_8));
    return getIndexDir(parquetFile).resolve(illegalUniqueKey);
  }

  public FileIndexResult useIndex(Path parquetFile, Predicate paimonPredicate) throws IOException {
    Path indexDir = getIndexDir(parquetFile);
    if (!Files.exists(indexDir)) {
      return REMAIN;
    }
    try (FileIndexResultLoader loader = new FileIndexResultLoader(parquetFile)) {
      return paimonPredicate.visit(loader);
    }
  }

  private class FileIndexResultLoader implements PredicateVisitor<FileIndexResult>, Closeable {

    private final Path parquetFile;
    private final List<SeekableInputStream> openedStreams = new ArrayList<>();
    private final HashMap<FieldRef, Set<FileIndexReader>> indexCache = new HashMap<>();

    private FileIndexResultLoader(Path parquetFile) {
      this.parquetFile = parquetFile;
    }

    private Set<FileIndexReader> readIndex(String name, DataType type) {
      RowType.Builder builder = RowType.builder();
      builder.field(name, type);
      RowType rowType = builder.build();

      Path indexFilePath = getIndexFile(parquetFile, name);
      SeekableInputStream inputStream;
      try {
        inputStream = new LocalFileIO.LocalSeekableInputStream(indexFilePath.toFile());
      } catch (FileNotFoundException e) {
        return Collections.emptySet();
      }
      openedStreams.add(inputStream);
      FileIndexFormat.Reader reader = FileIndexFormat.createReader(inputStream, rowType);
      return reader.readColumnIndex(name);
    }

    private Set<FileIndexReader> getIndexReaders(FieldRef fieldRef) {
      return indexCache.computeIfAbsent(fieldRef, ref -> readIndex(ref.name(), ref.type()));
    }

    @Override
    public FileIndexResult visit(LeafPredicate predicate) {
      FileIndexResult compoundResult = REMAIN;
      FieldRef fieldRef = new FieldRef(predicate.index(), predicate.fieldName(), predicate.type());
      Set<FileIndexReader> indexReaders = getIndexReaders(fieldRef);
      for (FileIndexReader fileIndexReader : indexReaders) {
        FileIndexResult currentResult =
            predicate.function().visit(fileIndexReader, fieldRef, predicate.literals());
        compoundResult = compoundResult.and(currentResult);
        if (!compoundResult.remain()) {
          return compoundResult;
        }
      }
      return compoundResult;
    }

    @Override
    public FileIndexResult visit(CompoundPredicate predicate) {
      List<FileIndexResult> childResults =
          predicate.children().stream()
              .map(child -> child.visit(this))
              .collect(Collectors.toList());
      if (predicate.function() instanceof Or) {
        return childResults.stream().reduce(FileIndexResult::or).orElse(FileIndexResult.REMAIN);
      } else if (predicate.function() instanceof And) {
        return childResults.stream().reduce(FileIndexResult::and).orElse(FileIndexResult.REMAIN);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported compound predicate function: " + predicate.function());
      }
    }

    @Override
    public void close() throws IOException {
      try {
        AutoCloseables.close(openedStreams);
      } catch (Exception e) {
        throw new IOException("Failed to close opened streams", e);
      }
    }
  }
}
