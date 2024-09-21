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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;

public abstract class PipelineExecutor extends UnaryExecutor {

  public Batch compute(@WillClose Batch batch) throws ComputeException {
    getOutputSchema(); // Ensure initialized
    try (StopWatch watch = new StopWatch(getContext()::addPipelineComputeTime)) {
      Batch producedBatch = internalCompute(batch);
      getContext().addProducedRowNumber(producedBatch.getRowCount());
      return producedBatch;
    } finally {
      batch.close();
    }
  }

  protected abstract Batch internalCompute(@WillNotClose Batch batch) throws ComputeException;
}
