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
package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
class ScatterBatchStream implements BatchStream {

  final BatchStream stream;
  final AtomicLong nextSequenceNumber = new AtomicLong(0);
  final AtomicBoolean finished = new AtomicBoolean(false);

  public ScatterBatchStream(BatchStream stream) {
    this.stream = Objects.requireNonNull(stream);
  }

  @Override
  public synchronized BatchSchema getSchema() throws PhysicalException {
    return stream.getSchema();
  }

  @Override
  public synchronized boolean hasNext() throws PhysicalException {
    if (finished.get()) {
      return false;
    }
    if (!stream.hasNext()) {
      finished.set(true);
      return false;
    }
    return true;
  }

  @Override
  public synchronized Batch getNext() throws PhysicalException {
    Batch batch = stream.getNext();
    batch.setSequenceNumber(nextSequenceNumber.getAndIncrement());
    return batch;
  }

  @Override
  public void close() throws PhysicalException {}
}
