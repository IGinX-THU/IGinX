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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;

public class ProjectionExecutor extends PipelineExecutor {

  private final UnaryExecutorInitializer<List<PhysicalExpression>> initializer;
  private List<PhysicalExpression> expressions;

  public ProjectionExecutor(UnaryExecutorInitializer<List<PhysicalExpression>> initializer) {
    this.initializer = initializer;
  }

  @Override
  public String getDescription() {
    return "Projection" + expressions;
  }

  @Override
  public void close() {}

  @Override
  protected Batch internalCompute(@WillNotClose Batch batch) throws ComputeException {
    List<FieldVector> results = new ArrayList<>();
    try {
      for (PhysicalExpression expression : expressions) {
        FieldVector result = expression.invoke(getContext().getAllocator(), batch.raw());
        if (result.getMinorType() == Types.MinorType.STRUCT) {
          // unnest multi-column results
          try (StructVector structVector = (StructVector) result) {
            for (FieldVector fieldVector : structVector.getChildrenFromFields()) {
              results.add(ValueVectors.transfer(getContext().getAllocator(), fieldVector));
            }
          }
        } else {
          results.add(result);
        }
      }
    } catch (ComputeException e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    return new Batch(new VectorSchemaRoot(results));
  }

  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws ComputeException {
    expressions = initializer.initialize(getContext(), inputSchema);
    try (Batch result = internalCompute(inputSchema.emptyBatch(getContext().getAllocator()))) {
      return result.getSchema();
    }
  }
}
