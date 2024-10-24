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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import javax.annotation.WillNotClose;

public abstract class UnarySinkExecutor extends UnaryExecutor {

  private boolean finished = false;

  protected UnarySinkExecutor(ExecutorContext context, BatchSchema inputSchema) {
    super(context, inputSchema);
  }

  public void consume(@WillNotClose Batch batch) throws ComputeException {
    if (finished) {
      throw new IllegalStateException(this + " has been finished, cannot consume");
    }
    try (StopWatch watch = new StopWatch(context::addSinkConsumeTime)) {
      internalConsume(batch);
    }
    context.addConsumedRowNumber(batch.getRowCount());
  }

  public void finish() throws ComputeException {
    if (finished) {
      throw new IllegalStateException(this + " has been finished, cannot finish again");
    }
    try (StopWatch watch = new StopWatch(context::addSinkFinishTime)) {
      internalFinish();
    }
    finished = true;
  }

  public Batch produce() throws ComputeException {
    if (!canProduce()) {
      throw new IllegalStateException(getClass().getSimpleName() + " cannot produce");
    }
    Batch batch;
    try (StopWatch watch = new StopWatch(context::addSinkProduceTime)) {
      batch = internalProduce();
    }
    context.addProducedRowNumber(batch.getRowCount());
    return batch;
  }

  public abstract boolean canProduce() throws ComputeException;

  protected abstract void internalConsume(@WillNotClose Batch batch) throws ComputeException;

  protected abstract void internalFinish() throws ComputeException;

  protected abstract Batch internalProduce() throws ComputeException;
}
