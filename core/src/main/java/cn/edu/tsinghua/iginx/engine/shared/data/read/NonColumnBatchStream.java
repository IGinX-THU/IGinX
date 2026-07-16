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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.Collections;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;

public class NonColumnBatchStream implements BatchStream {

  private final int batchSize;
  private int remaining;

  public NonColumnBatchStream(int batchSize, int rowCount) {
    Preconditions.checkArgument(batchSize > 0);
    Preconditions.checkArgument(rowCount >= 0);
    this.batchSize = batchSize;
    this.remaining = rowCount;
  }

  @Override
  public BatchSchema getSchema() throws PhysicalException {
    return BatchSchema.of(new Schema(Collections.emptyList()));
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return remaining > 0;
  }

  @Override
  public Batch getNext() throws PhysicalException {
    int size = Math.min(batchSize, remaining);
    remaining -= size;
    return Batch.of(VectorSchemaRoots.create(Collections.emptyList(), size));
  }

  @Override
  public void close() {}
}
