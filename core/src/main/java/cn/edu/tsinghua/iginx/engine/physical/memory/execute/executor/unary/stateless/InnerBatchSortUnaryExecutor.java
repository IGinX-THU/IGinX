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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.sort.IndexSortExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import java.util.Objects;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Schema;

public class InnerBatchSortUnaryExecutor extends StatelessUnaryExecutor {

  private final IndexSortExpression indexSortExpression;

  public InnerBatchSortUnaryExecutor(
      ExecutorContext context, Schema inputSchema, IndexSortExpression indexSortExpression) {
    super(context, inputSchema);
    this.indexSortExpression = Objects.requireNonNull(indexSortExpression);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  @Override
  protected String getInfo() {
    return "SortBatch by " + indexSortExpression;
  }

  @Override
  public void close() throws ComputeException {}

  @Override
  public Batch computeImpl(Batch batch) throws ComputeException {
    try (IntVector sortedIndices =
        ScalarExpressions.evaluate(
            context.getAllocator(),
            batch.getDictionaryProvider(),
            batch.getData(),
            batch.getSelection(),
            indexSortExpression)) {
      if (batch.getSelection() == null) {
        return batch.sliceWith(
            context.getAllocator(), ValueVectors.transfer(context.getAllocator(), sortedIndices));
      }
      BaseIntVector sortedSelection =
          PhysicalFunctions.take(context.getAllocator(), sortedIndices, batch.getSelection());
      return batch.sliceWith(context.getAllocator(), sortedSelection);
    }
  }
}
