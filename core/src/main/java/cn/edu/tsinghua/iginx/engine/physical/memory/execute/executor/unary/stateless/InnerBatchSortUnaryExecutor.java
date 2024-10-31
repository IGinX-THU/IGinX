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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.sort.IndexSortExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class InnerBatchSortUnaryExecutor extends StatelessUnaryExecutor {

  private final IndexSortExpression indexSortExpression;
  private final List<ScalarExpression<?>> outputExpressions;

  public InnerBatchSortUnaryExecutor(
      ExecutorContext context,
      Schema inputSchema,
      IndexSortExpression indexSortExpression,
      List<? extends ScalarExpression<?>> outputExpressions) {
    super(context, inputSchema);
    this.indexSortExpression = Objects.requireNonNull(indexSortExpression);
    this.outputExpressions = new ArrayList<>(outputExpressions);
  }

  @Override
  protected String getInfo() {
    return "InnerBatchSort" + outputExpressions + "By" + indexSortExpression.getOptions();
  }

  @Override
  public void close() throws ComputeException {}

  @Override
  public VectorSchemaRoot compute(VectorSchemaRoot batch) throws ComputeException {
    try (IntVector sortedIndices =
            ScalarExpressions.evaluateSafe(context.getAllocator(), indexSortExpression, batch);
        VectorSchemaRoot output =
            ScalarExpressions.evaluateSafe(context.getAllocator(), outputExpressions, batch)) {
      return PhysicalFunctions.take(context.getAllocator(), sortedIndices, output);
    }
  }
}
