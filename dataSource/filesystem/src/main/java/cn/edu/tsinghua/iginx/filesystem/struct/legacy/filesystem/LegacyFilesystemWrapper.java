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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Fields;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;

public class LegacyFilesystemWrapper implements FileManager {

  private final LocalExecutor executor;

  public LegacyFilesystemWrapper(@WillCloseWhenClosed LocalExecutor executor) {
    this.executor = executor;
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    Pair<ColumnsInterval, KeyInterval> boundary = executor.getBoundaryOfStorage(prefix);
    KeyInterval keyInterval = boundary.v;
    ColumnsInterval columnsInterval = boundary.k;
    DataBoundary dataBoundary =
        new DataBoundary(keyInterval.getStartKey(), keyInterval.getEndKey());
    dataBoundary.setStartColumn(columnsInterval.getStartColumn());
    dataBoundary.setEndColumn(columnsInterval.getEndColumn());
    return dataBoundary;
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("LegacyFilesystem does not support aggregate");
    }

    TagFilter tagFilter = target.getTagFilter(); // tagFilter is ignored

    Filter filter = target.getFilter();
    if (Filters.isTrue(filter)) {
      filter =
          new AndFilter(
              Arrays.asList(
                  new KeyFilter(Op.GE, Long.MIN_VALUE), new KeyFilter(Op.LE, Long.MAX_VALUE)));
    }

    List<String> patterns = target.getPatterns();
    if (Patterns.isAll(patterns)) {
      patterns = Collections.singletonList("*");
    }

    if (Filters.isFalse(target.getFilter())) {
      try {
        List<Column> columns =
            executor.getColumnsOfStorageUnit("*", new HashSet<>(patterns), tagFilter);
        List<Field> fields = columns.stream().map(Fields::of).collect(Collectors.toList());
        Header header = new Header(Field.KEY, fields);
        return new Table(header, Collections.emptyList());
      } catch (PhysicalException e) {
        throw new IOException("failed to show columns", e);
      }
    }

    TaskExecuteResult result = executor.executeDummyProjectTask(patterns, filter);
    if (result.getException() != null) {
      throw new IOException(result.getException());
    }
    return result.getRowStream();
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    throw new UnsupportedOperationException("LegacyFilesystem does not support delete");
  }

  @Override
  public void insert(DataView data) throws IOException {
    throw new UnsupportedOperationException("LegacyFilesystem does not support insert");
  }

  @Override
  public void close() throws IOException {
    executor.close();
  }
}
