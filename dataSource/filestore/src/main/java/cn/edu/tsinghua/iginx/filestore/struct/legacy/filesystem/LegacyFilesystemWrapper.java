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
package cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;

public class LegacyFilesystemWrapper implements FileManager {

  private final LocalExecutor executor;

  public LegacyFilesystemWrapper(@WillCloseWhenClosed LocalExecutor executor) {
    this.executor = executor;
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    try {
      Pair<ColumnsInterval, KeyInterval> boundary = executor.getBoundaryOfStorage(prefix);
      KeyInterval keyInterval = boundary.v;
      ColumnsInterval columnsInterval = boundary.k;
      DataBoundary dataBoundary =
          new DataBoundary(keyInterval.getStartKey(), keyInterval.getEndKey());
      dataBoundary.setStartColumn(columnsInterval.getStartColumn());
      dataBoundary.setEndColumn(columnsInterval.getEndColumn());
      return dataBoundary;
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("LegacyFilesystem does not support aggregate");
    }

    Filter filter = target.getFilter();
    List<String> patterns = target.getPatterns();
    TagFilter tagFilter = target.getTagFilter(); // tagFilter is ignored

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
