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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Fields;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.common.Ranges;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.exception.NoSuchUnitException;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.manager.data.DataManager;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyParquetWrapper implements FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyParquetWrapper.class);

  public interface DataManagerFactory {
    DataManager create(Path dir) throws IOException;
  }

  private final DataManagerFactory factory;
  private final Path path;
  private final boolean isDummy;
  private volatile long lastModified;
  private DataManager delegate;

  public LegacyParquetWrapper(DataManagerFactory factory, Path path, boolean isDummy)
      throws IOException {
    this.factory = Objects.requireNonNull(factory);
    this.path = Objects.requireNonNull(path);
    this.isDummy = isDummy;
    this.lastModified = System.currentTimeMillis();
    this.delegate = factory.create(path);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    if (!isDummy) {
      throw new UnsupportedOperationException(
          "getBoundary is not supported for non-dummy file manager");
    }
    try {
      List<Column> columns = delegate.getColumns(Collections.singletonList("*"), null);
      List<String> paths = columns.stream().map(Column::getPath).collect(Collectors.toList());
      if (prefix != null) {
        paths = paths.stream().filter(path -> path.startsWith(prefix)).collect(Collectors.toList());
      }
      paths.sort(String::compareTo);
      if (paths.isEmpty()) {
        return new DataBoundary();
      }
      ColumnsInterval columnsInterval =
          new ColumnsInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));
      KeyInterval keyInterval = KeyInterval.getDefaultKeyInterval();
      DataBoundary boundary = new DataBoundary(keyInterval.getStartKey(), keyInterval.getEndKey());
      boundary.setStartColumn(columnsInterval.getStartColumn());
      boundary.setEndColumn(columnsInterval.getEndColumn());
      return boundary;
    } catch (StorageException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (isDummy) {
      reload();
    }
    try {
      Filter filter = target.getFilter();
      if (Filters.isTrue(filter)) {
        filter = new BoolFilter(true);
      }
      List<String> patterns = target.getPatterns();
      TagFilter tagFilter = target.getTagFilter();

      if (Patterns.isAll(patterns)) {
        patterns = Collections.singletonList("*");
      }

      if (aggregate != null) {
        if (!Filters.isTrue(filter)) {
          throw new UnsupportedOperationException("Filter is not supported for aggregation");
        }
        return delegate.aggregation(patterns, tagFilter, null);
      } else {
        if (Filters.isFalse(target.getFilter())) {
          List<Column> columns = delegate.getColumns(patterns, tagFilter);
          List<Field> fields = columns.stream().map(Fields::of).collect(Collectors.toList());
          Header header = new Header(Field.KEY, fields);
          return new Table(header, Collections.emptyList());
        }
        RowStream rowStream = delegate.project(patterns, tagFilter, filter);
        rowStream = new ClearEmptyRowStreamWrapper(rowStream);
        if (!Filters.isTrue(filter)) {
          rowStream = new FilterRowStreamWrapper(rowStream, filter);
        }
        return rowStream;
      }
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  private synchronized void reload() throws IOException {
    while (true) {
      try {
        FileTime lastModifiedTime = Files.getLastModifiedTime(path);
        long lastModified = lastModifiedTime.toMillis();
        if (lastModified <= this.lastModified) {
          return;
        }
        LOGGER.info("reloading {} at {}", path, lastModifiedTime);
        this.lastModified = lastModified;
      } catch (NoSuchFileException e) {
        throw new NoSuchUnitException(e);
      }
      close();
      delegate = factory.create(path);
    }
  }

  private void touch() {
    if (isDummy) {
      throw new UnsupportedOperationException("touch is not supported for dummy file manager");
    }
    try {
      Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis()));
    } catch (NoSuchFileException ignored) {
    } catch (IOException e) {
      LOGGER.error("Failed to touch file {}", path, e);
    }
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    if (isDummy) {
      throw new UnsupportedOperationException("delete is not supported for dummy file manager");
    }
    try {
      List<KeyRange> keyRanges = null;
      RangeSet<Long> rangeSet = Filters.toRangeSet(target.getFilter());
      if (!rangeSet.encloses(Range.all())) {
        keyRanges = Ranges.toKeyRanges(rangeSet);
      }
      List<String> patterns = target.getPatterns();
      if (Patterns.isAll(patterns)) {
        patterns = Collections.singletonList("*");
      }
      delegate.delete(patterns, keyRanges, target.getTagFilter());
      touch();
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void insert(DataView data) throws IOException {
    if (isDummy) {
      throw new UnsupportedOperationException("insert is not supported for dummy file manager");
    }
    try {
      delegate.insert(data);
      touch();
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      delegate.close();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
