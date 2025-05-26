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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.arrow.util.Preconditions;

public class BatchBufferQueue implements AutoCloseable {

  private final int backlog;
  private final Queue<Batch> queue = new ArrayDeque<>();
  private long sequenceNumber = 0;

  public BatchBufferQueue(int backlog) {
    Preconditions.checkArgument(backlog > 0, "Backlog must be positive");
    this.backlog = backlog;
  }

  public boolean isFull() {
    return queue.size() >= backlog;
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public void add(ExecutorContext context, Batch batch) {
    for (Batch partition :
        Batches.partition(context.getAllocator(), batch, context.getBatchRowCount())) {
      partition.setSequenceNumber(sequenceNumber++);
      queue.add(partition);
    }
  }

  public Batch remove() {
    return queue.remove();
  }

  @Override
  public void close() {
    queue.forEach(Batch::close);
    queue.clear();
  }
}
