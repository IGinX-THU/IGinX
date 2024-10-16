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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.select.VectorFilter;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FilterExecutor extends PipelineExecutor {

  private final UnaryExecutorInitializer<PhysicalExpression> initializer;
  private final VectorFilter filter = new VectorFilter();
  private PhysicalExpression expression;

  public FilterExecutor(UnaryExecutorInitializer<PhysicalExpression> initializer) {
    this.initializer = Objects.requireNonNull(initializer);
  }

  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws ComputeException {
    expression = initializer.initialize(getContext(), inputSchema);
    return inputSchema;
  }

  @Override
  protected Batch internalCompute(Batch batch) throws ComputeException {
    List<FieldVector> results = new ArrayList<>();
    try (BitVector mask = expression.evaluate(getContext(), batch.raw(), BitVector.class)) {
      for (FieldVector fieldVector : batch.raw().getFieldVectors()) {
        results.add(filter.evaluate(getContext(), mask, fieldVector));
      }
    } catch (Exception e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    return new Batch(new VectorSchemaRoot(results));
  }

  @Override
  public String getDescription() {
    return "Filter(" + expression + ")";
  }

  @Override
  public void close() {
  }
}
