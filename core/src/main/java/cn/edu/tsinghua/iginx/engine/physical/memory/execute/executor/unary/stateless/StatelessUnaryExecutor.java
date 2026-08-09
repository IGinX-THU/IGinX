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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class StatelessUnaryExecutor extends UnaryExecutor {

  protected final boolean empty;

  protected StatelessUnaryExecutor(ExecutorContext context, Schema inputSchema) {
    this(context, inputSchema, false);
  }

  protected StatelessUnaryExecutor(ExecutorContext context, Schema inputSchema, boolean empty) {
    super(context, inputSchema);
    this.empty = empty;
  }

  public Batch compute(Batch batch) throws ComputeException {
    Batch result = computeImpl(batch);
    result.setSequenceNumber(batch.getSequenceNumber());
    return result;
  }

  public boolean isEmpty() {
    return empty;
  }

  protected abstract Batch computeImpl(Batch batch) throws ComputeException;
}
