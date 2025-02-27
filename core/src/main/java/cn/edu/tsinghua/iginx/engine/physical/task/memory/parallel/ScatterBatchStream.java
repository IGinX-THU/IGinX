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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ScatterBatchStream implements BatchStream {

  private final BatchStream stream;
  private long nextSequenceNumber = 0;
  private boolean closed = false;

  public ScatterBatchStream(BatchStream stream) {
    this.stream = Objects.requireNonNull(stream);
  }

  @Override
  public synchronized BatchSchema getSchema() throws PhysicalException {
    if (closed) {
      throw new IllegalStateException("ScatterBatchStream has been closed");
    }
    return stream.getSchema();
  }

  @Override
  public synchronized boolean hasNext() throws PhysicalException {
    return !closed && stream.hasNext();
  }

  @Override
  public synchronized Batch getNext() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Batch batch = stream.getNext();
    batch.setSequenceNumber(nextSequenceNumber);
    nextSequenceNumber++;
    return batch;
  }

  @Override
  public synchronized void close() throws PhysicalException {
    closed = true;
    stream.close();
  }

  public synchronized long getNextSequenceNumber() {
    return nextSequenceNumber;
  }
}
