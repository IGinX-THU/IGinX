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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskMetrics;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

public class FetchMetricsRowStream implements RowStream {

  private final RowStream delegate;
  private final TaskMetrics metrics;
  private final int batchRowCount;
  private final Queue<Row> cache = new ArrayDeque<>();

  public FetchMetricsRowStream(RowStream delegate, TaskMetrics metrics, int batchRowCount) {
    this.delegate = Objects.requireNonNull(delegate);
    this.metrics = Objects.requireNonNull(metrics);
    this.batchRowCount = batchRowCount;
  }

  private Header header;

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      header = delegate.getHeader();
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return !cache.isEmpty() || delegate.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (cache.isEmpty() && delegate.hasNext()) {
      fetchBatch();
    }
    return cache.remove();
  }

  private void fetchBatch() throws PhysicalException {
    try (StopWatch watch = new StopWatch(metrics::accumulateCpuTime)) {
      for (int i = 0; i < batchRowCount && delegate.hasNext(); i++) {
        cache.add(delegate.next());
      }
    }
    metrics.accumulateAffectRows(cache.size());
  }

  @Override
  public void close() throws PhysicalException {
    delegate.close();
  }
}
