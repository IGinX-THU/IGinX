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
import jdk.nashorn.internal.ir.annotations.Immutable;

@Immutable
public abstract class PipelineExecutor implements PhysicalExecutor {

  public abstract BatchSchema getOutputSchema(ExecutorContext context, BatchSchema inputSchema)
      throws PhysicalException;

  public Batch compute(ExecutorContext context, @WillClose Batch batch) throws PhysicalException {
    try {
      long start = System.currentTimeMillis();
      Batch producedBatch = computeWithoutTimer(context, batch);
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      context.accumulateCpuTime(elapsed);
      context.accumulateProducedRows(producedBatch.getRowCount());
      return producedBatch;
    } finally {
      batch.close();
    }
  }

  protected abstract Batch computeWithoutTimer(ExecutorContext context, @WillNotClose Batch batch)
      throws PhysicalException;
}
