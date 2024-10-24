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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FilterExecutor extends PipelineExecutor {

  private final ScalarExpression<BitVector> condition;
  private final List<ScalarExpression<?>> outputExpressions;

  public FilterExecutor(
      ExecutorContext context,
      BatchSchema inputSchema,
      ScalarExpression<BitVector> condition,
      List<? extends ScalarExpression<?>> outputExpressions) {
    super(context, inputSchema);
    this.condition = Objects.requireNonNull(condition);
    this.outputExpressions = new ArrayList<>(outputExpressions);
  }

  @Override
  protected Batch internalCompute(Batch batch) throws ComputeException {
    try (BitVector mask =
            ScalarExpressions.evaluateSafe(context.getAllocator(), condition, batch.raw());
        IntVector selection = PhysicalFunctions.filter(context.getAllocator(), mask);
        VectorSchemaRoot output =
            ScalarExpressions.evaluateSafe(
                context.getAllocator(), outputExpressions, batch.raw())) {
      VectorSchemaRoot filteredOutput =
          PhysicalFunctions.take(context.getAllocator(), selection, output);
      return new Batch(filteredOutput);
    }
  }

  @Override
  public String getInfo() {
    return "Filter(" + condition + ")";
  }

  @Override
  public void close() {}
}
