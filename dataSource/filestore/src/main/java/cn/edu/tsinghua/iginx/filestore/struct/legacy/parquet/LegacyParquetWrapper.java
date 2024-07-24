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
package cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filestore.common.Fields;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.common.Patterns;
import cn.edu.tsinghua.iginx.filestore.common.Ranges;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.data.DataManager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LegacyParquetWrapper implements FileManager {

  private final DataManager delegate;
  private final boolean isDummy;

  public LegacyParquetWrapper(@WillCloseWhenClosed DataManager delegate, boolean isDummy) {
    this.delegate = Objects.requireNonNull(delegate);
    this.isDummy = isDummy;
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    if (!isDummy) {
      throw new UnsupportedOperationException("getBoundary is not supported for non-dummy file manager");
    }
    try {
      List<Column> columns = delegate.getColumns();
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
    try {
      Filter filter = target.getFilter();
      if (Filters.isTrue(filter)) {
        filter = null;
      }
      List<String> patterns = target.getPatterns();
      TagFilter tagFilter = target.getTagFilter();

      if (aggregate != null) {
        if (filter != null) {
          throw new UnsupportedOperationException("Filter is not supported for aggregation");
        }
        return delegate.aggregation(patterns, tagFilter, null);
      } else {
        if (tagFilter == null && Filters.isFalse(target.getFilter())) {
          List<Column> columns = delegate.getColumns();
          List<Field> fields = columns.stream().map(Fields::of).collect(Collectors.toList());
          Header header = new Header(Field.KEY, fields);
          return new Table(header, Collections.emptyList());
        }
        return delegate.project(patterns, tagFilter, filter);
      }
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(DataTarget target) throws IOException {
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
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void insert(DataView data) throws IOException {
    try {
      delegate.insert(data);
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      delegate.close();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
