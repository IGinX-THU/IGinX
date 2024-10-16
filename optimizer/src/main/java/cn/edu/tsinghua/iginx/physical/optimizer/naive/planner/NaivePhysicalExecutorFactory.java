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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.planner;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorType;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.FilterExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.PipelineExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.ProjectionExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink.AggregateExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.sink.UnarySinkExecutor;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;

public class NaivePhysicalExecutorFactory {

  public ExecutorType getExecutorType(Operator operator) {
    switch (operator.getType()) {
      case Project:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:
      case RowTransform:
      case Select:
        return ExecutorType.Pipeline;
      case SetTransform:
        return ExecutorType.UnarySink;
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  public PipelineExecutor createPipelineExecutor(UnaryOperator operator) {
    switch (operator.getType()) {
      case Project:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:
        return new ProjectionExecutor(new SimpleProjectionInfoGenerator(operator));
      case RowTransform:
        return new ProjectionExecutor(
            new TransformProjectionInfoGenerator((RowTransform) operator));
      case Select:
        return new FilterExecutor(new FilterInfoGenerator((Select) operator));
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  public UnarySinkExecutor createUnarySinkExecutor(UnaryOperator operator) {
    switch (operator.getType()) {
      case SetTransform:
        return new AggregateExecutor(new AggregateInfoGenerator((SetTransform) operator));
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }
}
