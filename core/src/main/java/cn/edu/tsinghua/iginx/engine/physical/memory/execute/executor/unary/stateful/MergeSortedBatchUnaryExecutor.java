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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import org.apache.arrow.vector.types.pojo.Schema;

public class MergeSortedBatchUnaryExecutor extends StatefulUnaryExecutor {

  public MergeSortedBatchUnaryExecutor(ExecutorContext context, Schema inputSchema) {
    super(context, inputSchema, 1);
  }

  @Override
  protected String getInfo() {
    return "MergeSortedBatch";
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  private boolean consumed = false;

  @Override
  protected void consumeUnchecked(Batch batch) throws ComputeException {
    if (consumed) {
      throw new ComputeException("MergeSortedBatch can't merge more than one batch now");
    }
    consumed = true;
    offerResult(batch.slice(context.getAllocator()));
  }

  @Override
  protected void consumeEnd() throws ComputeException {}
}
