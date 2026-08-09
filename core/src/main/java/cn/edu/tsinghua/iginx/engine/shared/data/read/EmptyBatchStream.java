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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.NoSuchElementException;
import java.util.Objects;

class EmptyBatchStream implements BatchStream {

  private final BatchSchema schema;

  public EmptyBatchStream(BatchSchema schema) {
    this.schema = Objects.requireNonNull(schema);
  }

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    return schema;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return false;
  }

  @Override
  public Batch getNext() throws PhysicalException {
    throw new NoSuchElementException("EmptyBatchStream has no next batch");
  }

  @Override
  public void close() {}
}
