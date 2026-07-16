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
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import oshi.annotation.concurrent.ThreadSafe;

@ThreadSafe
public abstract class PrefetchBatchStream implements BatchStream {

  private Batch batch;
  private boolean finished;

  @Override
  public synchronized boolean hasNext() throws PhysicalException {
    if (finished) {
      return false;
    }
    if (batch == null) {
      batch = prefetchBatch();
    }
    if (batch == null) {
      finished = true;
      return false;
    }
    return true;
  }

  @Override
  public synchronized Batch getNext() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Batch res = batch;
    batch = null;
    return res;
  }

  @Override
  public synchronized void close() throws PhysicalException {
    finished = true;
    if (batch != null) {
      batch.close();
    }
  }

  @Nullable
  protected abstract Batch prefetchBatch() throws PhysicalException;
}
