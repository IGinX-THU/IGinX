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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CloseableDictionaryProvider;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeResult;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.DictionaryProviders;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayDeque;
import java.util.Queue;

public abstract class StatefulUnaryExecutor extends UnaryExecutor {

  private final int backlog;
  private final Queue<Batch> results = new ArrayDeque<>();
  private boolean allConsumed = false;

  protected StatefulUnaryExecutor(ExecutorContext context, Schema inputSchema, int backlog) {
    super(context, inputSchema);
    Preconditions.checkArgument(backlog > 0, "Backlog must be positive");
    this.backlog = backlog;
  }

  @Override
  public void close() throws ComputeException {
    results.forEach(Batch::close);
    results.clear();
  }

  /**
   * Check if the executor needs to consume more data.
   *
   * @return true if the executor needs to consume more data, false otherwise
   * @throws ComputeException if an error occurs during consumption
   */
  public boolean needConsume() throws ComputeException {
    return !allConsumed && results.size() < backlog;
  }

  /**
   * Consume a batch of data.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size
   *              less than the batch size
   * @throws ComputeException      if an error occurs during consumption
   * @throws IllegalStateException if the executor is not ready to consume, i.e., need to produce
   *                               the result
   */
  public void consume(Batch batch) throws ComputeException {
    if (!needConsume()) {
      throw new IllegalStateException("Executor does not need to consume more data");
    }
    if (batch.getRowCount() == 0) {
      allConsumed = true;
      consumeEnd();
    } else {
      consumeUnchecked(batch);
    }
  }

  /**
   * Produce the result of the computation.
   *
   * @return the result of the computation. Empty if executor has produced all results
   * @throws ComputeException if an error occurs during consumption
   */
  public Batch produce() throws ComputeException {
    if (results.isEmpty()) {
      if (needConsume()) {
        throw new IllegalStateException(
            "Executor "
                + getClass().getSimpleName()
                + " does not have enough data to produce the result");
      }
      return Batch.empty(context.getAllocator(), getOutputSchema());
    }
    return results.remove();
  }

  protected void offerResult(Batch batch) {
    results.add(batch);
  }

  protected abstract void consumeUnchecked(Batch batch) throws ComputeException;

  protected abstract void consumeEnd() throws ComputeException;
}
