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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful.FilterUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.Filters;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.Generators;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.BitVector;

public class FilterInfoGenerator implements UnaryExecutorFactory<FilterUnaryExecutor> {

  private final Filter filter;

  public FilterInfoGenerator(Filter filter) {
    this.filter = Objects.requireNonNull(filter);
  }

  @Override
  public FilterUnaryExecutor initialize(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    ScalarExpression<BitVector> condition = getCondition(context, inputSchema);
    List<FieldNode> outputExpressions = Generators.allFieldExpressions(inputSchema.getFieldCount());
    return new FilterUnaryExecutor(context, inputSchema.raw(), condition, outputExpressions);
  }

  public ScalarExpression<BitVector> getCondition(ExecutorContext context, BatchSchema inputSchema)
      throws ComputeException {
    return Filters.construct(filter, context, inputSchema);
  }
}
