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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class StatefulBinaryExecutor extends BinaryExecutor {

  private final int backlog;
  private final Queue<VectorSchemaRoot> results = new ArrayDeque<>();
  private boolean leftAllConsumed = false;
  private boolean rightAllConsumed = false;

  protected StatefulBinaryExecutor(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema, int backlog) {
    super(context, leftSchema, rightSchema);
    Preconditions.checkArgument(backlog > 0, "Backlog must be positive");
    this.backlog = backlog;
  }

  public boolean needConsumeLeft() throws ComputeException {
    return !leftAllConsumed && results.size() < backlog;
  }

  public boolean needConsumeRight() throws ComputeException {
    return !rightAllConsumed && results.size() < backlog;
  }

  /**
   * Consume a batch of data from the left child.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size
   *     less than the batch size
   * @throws ComputeException if an error occurs during consumption
   */
  public void consumeLeft(VectorSchemaRoot batch) throws ComputeException {
    if (!needConsumeLeft()) {
      throw new IllegalStateException(
          "Executor does not need to consume more data from the left child");
    }
    if (batch.getRowCount() == 0) {
      consumeLeftEnd();
      leftAllConsumed = true;
    } else {
      consumeLeftUnchecked(batch);
    }
  }

  /**
   * Consume a batch of data from the right child.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size
   *     less than the batch size
   * @throws ComputeException if an error occurs during consumption
   */
  public void consumeRight(VectorSchemaRoot batch) throws ComputeException {
    if (!needConsumeRight()) {
      throw new IllegalStateException(
          "Executor does not need to consume more data from the right child");
    }
    if (batch.getRowCount() == 0) {
      consumeRightEnd();
      rightAllConsumed = true;
    } else {
      consumeRightUnchecked(batch);
    }
  }

  /**
   * Produce the result of the computation.
   *
   * @return the result of the computation. Empty if executor has produced all results
   */
  public VectorSchemaRoot produce() throws ComputeException {
    if (results.isEmpty()) {
      if (needConsumeLeft() || needConsumeRight()) {
        throw new IllegalStateException(
            "Executor "
                + getClass().getSimpleName()
                + " does not have enough data to produce the result");
      }
      return VectorSchemaRoot.create(getOutputSchema(), context.getAllocator());
    }
    return results.remove();
  }

  protected void offerResult(VectorSchemaRoot batch) {
    results.add(batch);
  }

  protected abstract void consumeLeftUnchecked(VectorSchemaRoot batch) throws ComputeException;

  protected abstract void consumeLeftEnd() throws ComputeException;

  protected abstract void consumeRightUnchecked(VectorSchemaRoot batch) throws ComputeException;

  protected abstract void consumeRightEnd() throws ComputeException;
}
