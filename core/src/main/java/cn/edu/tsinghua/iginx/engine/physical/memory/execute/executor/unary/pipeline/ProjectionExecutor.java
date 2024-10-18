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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ProjectionExecutor extends PipelineExecutor {

  protected final List<ScalarExpression> expressions;

  public ProjectionExecutor(
      ExecutorContext context, BatchSchema inputSchema, List<ScalarExpression> expressions) {
    super(context, inputSchema);
    this.expressions = new ArrayList<>(expressions);
  }

  @Override
  public String getInfo() {
    return "Project" + expressions;
  }

  @Override
  public void close() {}

  @Override
  protected Batch internalCompute(@WillNotClose Batch batch) throws ComputeException {
    try (VectorSchemaRoot result =
        ScalarExpressions.evaluateSafe(context.getAllocator(), expressions, batch.raw())) {
      return new Batch(PhysicalFunctions.unnest(context.getAllocator(), result));
    }
  }
}
