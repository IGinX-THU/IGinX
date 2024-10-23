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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.CastAsBit;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.Objects;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FilterExecutor extends PipelineExecutor {

  private final ScalarExpression condition;
  private final CastAsBit castAsBit = new CastAsBit();

  public FilterExecutor(
      ExecutorContext context, BatchSchema inputSchema, ScalarExpression condition) {
    super(context, inputSchema);
    this.condition = Objects.requireNonNull(condition);
  }

  @Override
  protected Batch internalCompute(Batch batch) throws ComputeException {
    try (FieldVector mask = condition.invoke(context.getAllocator(), batch.raw());
        BitVector bitmask = castAsBit.evaluate(context.getAllocator(), mask);
        IntVector selection = PhysicalFunctions.filter(context.getAllocator(), bitmask)) {
      VectorSchemaRoot root =
          PhysicalFunctions.take(context.getAllocator(), selection, batch.raw());
      return new Batch(root);
    }
  }

  @Override
  public String getInfo() {
    return "Filter(" + condition + ")";
  }

  @Override
  public void close() {}
}
