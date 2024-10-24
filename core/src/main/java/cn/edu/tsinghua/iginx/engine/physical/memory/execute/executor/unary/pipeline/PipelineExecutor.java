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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;

public abstract class PipelineExecutor extends UnaryExecutor {

  protected PipelineExecutor(ExecutorContext context, BatchSchema inputSchema) {
    super(context, inputSchema);
  }

  private BatchSchema outputSchema;

  @Override
  public BatchSchema getOutputSchema() throws ComputeException {
    if (outputSchema == null) {
      try (Batch emptyBatch = inputSchema.emptyBatch(context.getAllocator())) {
        try (Batch producedBatch = compute(emptyBatch)) {
          outputSchema = producedBatch.getSchema();
        }
      }
    }
    return outputSchema;
  }

  public Batch compute(@WillClose Batch batch) throws ComputeException {
    try (StopWatch watch = new StopWatch(context::addPipelineComputeTime)) {
      Batch producedBatch = internalCompute(batch);
      context.addProducedRowNumber(producedBatch.getRowCount());
      return producedBatch;
    } finally {
      batch.close();
    }
  }

  protected abstract Batch internalCompute(@WillNotClose Batch batch) throws ComputeException;
}
