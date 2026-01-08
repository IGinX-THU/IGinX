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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import org.apache.arrow.vector.types.pojo.Schema;

public class FetchAllUnaryExecutor extends StatefulUnaryExecutor {

  public FetchAllUnaryExecutor(ExecutorContext context, Schema inputSchema) {
    super(context, inputSchema, Integer.MAX_VALUE);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  @Override
  protected String getInfo() {
    return "FetchAll";
  }

  @Override
  protected void consumeUnchecked(Batch batch) throws ComputeException {
    offerResult(batch);
  }

  @Override
  protected void consumeEndUnchecked() throws ComputeException {}
}
