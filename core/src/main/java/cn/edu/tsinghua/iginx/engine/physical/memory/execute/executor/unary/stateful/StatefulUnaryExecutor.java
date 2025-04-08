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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.BatchBufferQueue;
import java.util.NoSuchElementException;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class StatefulUnaryExecutor extends UnaryExecutor {

  private final BatchBufferQueue results;
  private boolean allConsumed = false;

  protected StatefulUnaryExecutor(ExecutorContext context, Schema inputSchema, int backlog) {
    super(context, inputSchema);
    this.results = new BatchBufferQueue(backlog);
  }

  @Override
  public void close() throws ComputeException {
    results.close();
  }

  /**
   * Check if the executor needs to consume more data.
   *
   * @return true if the executor needs to consume more data, false otherwise
   * @throws ComputeException if an error occurs during consumption
   */
  public boolean needConsume() throws ComputeException {
    return !allConsumed && !results.isFull();
  }

  /**
   * Consume a batch of data.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size
   *     less than the batch size
   * @throws ComputeException if an error occurs during consumption
   * @throws IllegalStateException if the executor is not ready to consume, i.e., need to produce
   *     the result
   */
  public void consume(Batch batch) throws ComputeException {
    if (!needConsume()) {
      throw new IllegalStateException("Executor does not need to consume more data");
    }
    consumeUnchecked(batch);
  }

  public void consumeEnd() throws ComputeException {
    if (!needConsume()) {
      throw new IllegalStateException("Executor does not need to consume more data");
    }
    allConsumed = true;
    consumeEndUnchecked();
  }

  public boolean canProduce() {
    return !results.isEmpty();
  }

  /**
   * Produce the result of the computation.
   *
   * @return the result of the computation. Empty if executor has produced all results
   * @throws ComputeException if an error occurs during consumption
   */
  public Batch produce() throws ComputeException {
    if (!canProduce()) {
      throw new NoSuchElementException("No more results to produce");
    }
    return results.remove();
  }

  protected void offerResult(Batch batch) {
    results.add(context, batch);
  }

  protected abstract void consumeUnchecked(Batch batch) throws ComputeException;

  protected abstract void consumeEndUnchecked() throws ComputeException;
}
