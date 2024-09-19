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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.PhysicalExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;

public abstract class PipelineExecutor extends PhysicalExecutor {

  private BatchSchema outputSchema;

  public void internalInitialize(ExecutorContext context, BatchSchema inputSchema)
      throws PhysicalException {
    super.initialize(context);
    outputSchema = internalInitialize(inputSchema);
  }

  public BatchSchema getOutputSchema() {
    if (outputSchema == null) {
      throw new IllegalStateException("Not initialized");
    }
    return outputSchema;
  }

  public Batch compute(@WillClose Batch batch) throws PhysicalException {
    try {
      long start = System.currentTimeMillis();
      Batch producedBatch = internalCompute(batch);
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      getContext().accumulateCpuTime(elapsed);
      getContext().accumulateProducedRows(producedBatch.getRowCount());
      return producedBatch;
    } finally {
      batch.close();
    }
  }

  protected abstract BatchSchema internalInitialize(BatchSchema inputSchema)
      throws PhysicalException;

  protected abstract Batch internalCompute(@WillNotClose Batch batch) throws PhysicalException;
}
