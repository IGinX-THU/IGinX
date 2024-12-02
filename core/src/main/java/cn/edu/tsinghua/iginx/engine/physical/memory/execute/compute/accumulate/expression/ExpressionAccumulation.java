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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulation;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

public class ExpressionAccumulation implements Accumulation {

  private final Accumulation accumulation;
  private final List<? extends ScalarExpression<?>> expressions;

  public ExpressionAccumulation(
      Accumulation accumulation, List<? extends ScalarExpression<?>> expressions)
      throws ComputeException {
    this.accumulation = Objects.requireNonNull(accumulation);
    this.expressions = Objects.requireNonNull(expressions);
  }

  @Override
  public String getName() {
    return accumulation.getName()
        + expressions.stream()
            .map(ScalarExpression::getName)
            .collect(Collectors.joining(", ", "(", ")"));
  }

  @Override
  public ExpressionAccumulator accumulate(BufferAllocator allocator, Schema inputSchema)
      throws ComputeException {
    Schema schema = ScalarExpressionUtils.getOutputSchema(allocator, expressions, inputSchema);
    Accumulator accumulator = accumulation.accumulate(allocator, schema);
    return new ExpressionAccumulator(allocator, accumulator, expressions);
  }
}
