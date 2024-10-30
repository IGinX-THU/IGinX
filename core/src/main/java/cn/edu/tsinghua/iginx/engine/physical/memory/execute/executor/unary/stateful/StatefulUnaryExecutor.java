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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.WillNotClose;

public abstract class StatefulUnaryExecutor extends UnaryExecutor {

  protected StatefulUnaryExecutor(ExecutorContext context, Schema inputSchema) {
    super(context, inputSchema);
  }

  /**
   * Check if the executor needs to consume more data.
   *
   * @return true if the executor needs to consume more data, false otherwise
   * @throws ComputeException if an error occurs during consumption
   */
  public abstract boolean needConsume() throws ComputeException;

  /**
   * Consume a batch of data.
   *
   * @param batch the batch to consume, notify the consumer to finalize states if the batch's size less than the batch size
   * @throws ComputeException if an error occurs during consumption
   * @throws IllegalStateException if the executor is not ready to consume, i.e., need to produce the result
   */
  public abstract void consume(@WillNotClose VectorSchemaRoot batch) throws ComputeException;

  /**
   * Produce the result of the computation.
   *
   * @return the result of the computation. Empty if executor needs to consume more data.
   * @throws ComputeException      if an error occurs during consumption
   * @throws IllegalStateException if the executor is not ready to produce, i.e., need to consume more data
   */
  public abstract VectorSchemaRoot produce() throws ComputeException;
}
