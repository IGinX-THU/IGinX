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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.NoExceptionAutoCloseable;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import java.util.List;
import java.util.Objects;
import javax.annotation.WillNotClose;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

public class ProjectionExecutor extends PipelineExecutor {

  private final UnaryExecutorInitializer<List<ProjectionInfo>> initializer;
  private List<ProjectionInfo> projectionInfos;

  public static class ProjectionInfo implements NoExceptionAutoCloseable {
    private final Field field;
    private final PhysicalExpression expression;

    public ProjectionInfo(Field field, PhysicalExpression expression) {
      this.field = field;
      this.expression = expression;
    }

    public Field getField() {
      return field;
    }

    public PhysicalExpression getExpression() {
      return expression;
    }

    @Override
    public String toString() {
      return field + " <- " + expression.getName();
    }

    @Override
    public void close() {
      expression.close();
    }
  }

  public ProjectionExecutor(UnaryExecutorInitializer<List<ProjectionInfo>> initializer) {
    this.initializer = Objects.requireNonNull(initializer);
  }

  @Override
  public String getDescription() {
    return ProjectionExecutor.class.getSimpleName() + ": " + projectionInfos;
  }

  @Override
  public void close() {
    if (projectionInfos != null) {
      projectionInfos.forEach(ProjectionInfo::close);
    }
  }

  @Override
  protected Batch internalCompute(@WillNotClose Batch batch) throws ComputeException {
    try (Batch.Builder builder =
        new Batch.Builder(getContext().getAllocator(), getOutputSchema())) {
      ValueVector[] sourceVectors = batch.getVectors().toArray(new ValueVector[0]);
      int rowCount = batch.getRowCount();
      for (int targetIndex = 0; targetIndex < projectionInfos.size(); targetIndex++) {
        PhysicalExpression expression = projectionInfos.get(targetIndex).getExpression();
        try (ValueVector result = expression.invoke(getContext(), rowCount, sourceVectors)) {
          builder.getTransferPair(targetIndex, result).transfer();
        }
      }
      return builder.build(rowCount);
    }
  }

  @Override
  protected BatchSchema internalInitialize(BatchSchema inputSchema) throws ComputeException {
    projectionInfos = initializer.initialize(getContext(), inputSchema);
    BatchSchema.Builder builder = BatchSchema.builder();
    for (ProjectionInfo info : projectionInfos) {
      builder.addField(info.getField());
    }
    return builder.build();
  }
}
