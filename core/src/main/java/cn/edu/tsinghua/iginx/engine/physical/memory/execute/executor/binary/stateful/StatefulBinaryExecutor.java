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
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class StatefulBinaryExecutor extends BinaryExecutor {

  protected StatefulBinaryExecutor(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema) {
    super(context, leftSchema, rightSchema);
  }

  public abstract boolean needConsumeLeft() throws ComputeException;

  public abstract boolean needConsumeRight() throws ComputeException;

  /**
   * Consume a batch of data from the left child.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size
   *     less than the batch size
   * @throws ComputeException if an error occurs during consumption
   */
  public abstract void consumeLeft(@WillNotClose VectorSchemaRoot batch) throws ComputeException;

  /**
   * Consume a batch of data from the right child.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size
   *     less than the batch size
   * @throws ComputeException if an error occurs during consumption
   */
  public abstract void consumeRight(@WillNotClose VectorSchemaRoot batch) throws ComputeException;

  /**
   * Produce the result of the computation.
   *
   * @return the result of the computation. Empty if executor needs to consume more data.
   */
  public abstract VectorSchemaRoot produce() throws ComputeException;
}
