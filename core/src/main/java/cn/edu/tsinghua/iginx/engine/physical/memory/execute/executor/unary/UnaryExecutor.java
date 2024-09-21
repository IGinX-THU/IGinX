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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.PhysicalExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;

public abstract class UnaryExecutor extends PhysicalExecutor {

  private BatchSchema outputSchema;

  public void initialize(ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    super.initialize(context);
    if (outputSchema != null) {
      throw new IllegalStateException(getClass().getSimpleName() + " has been initialized");
    }
    try (StopWatch watch = new StopWatch(context::addInitializeTime)) {
      outputSchema = internalInitialize(inputSchema);
    }
  }

  public BatchSchema getOutputSchema() {
    if (outputSchema == null) {
      throw new IllegalStateException(getClass().getSimpleName() + " has not been initialized");
    }
    return outputSchema;
  }

  protected abstract BatchSchema internalInitialize(BatchSchema inputSchema)
      throws ComputeException;
}
