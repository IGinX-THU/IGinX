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
import javax.annotation.WillNotClose;

class PrefetchBatchStreamReference implements BatchStream {

  private final BatchStream stream;

  public PrefetchBatchStreamReference(@WillNotClose BatchStream stream) {
    this.stream = Objects.requireNonNull(stream);
  }

  private BatchSchema schema;

  private Batch prefetched;

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    if (schema == null) {
      schema = stream.getSchema();
    }
    return schema;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (prefetched == null) {
      prefetched = fetch();
    }
    return prefetched != null;
  }

  @Override
  public Batch getNext() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Batch result = prefetched;
    prefetched = null;
    return result;
  }

  private Batch fetch() throws PhysicalException {
    if (!stream.hasNext()) {
      return null;
    }
    try {
      return stream.getNext();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public void close() throws PhysicalException {
    if (prefetched != null) {
      prefetched.close();
    }
  }
}
