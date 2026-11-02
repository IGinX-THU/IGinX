/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressionUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ExpressionAccumulator implements Accumulator {

  private final BufferAllocator allocator;
  private final Accumulator accumulator;
  private final List<ScalarExpression<?>> expressions;

  ExpressionAccumulator(
      BufferAllocator allocator,
      Accumulator accumulator,
      List<? extends ScalarExpression<?>> expressions) {
    this.allocator = Objects.requireNonNull(allocator);
    this.accumulator = Objects.requireNonNull(accumulator);
    this.expressions = new ArrayList<>(expressions);
  }

  @Override
  public State createState() throws ComputeException {
    return accumulator.createState();
  }

  @Override
  public void update(State state, VectorSchemaRoot input) throws ComputeException {
    try (VectorSchemaRoot expressionResult =
        ScalarExpressionUtils.evaluate(allocator, input, expressions)) {
      accumulator.update(state, expressionResult);
    }
  }

  @Override
  public FieldVector evaluate(List<State> states) throws ComputeException {
    return accumulator.evaluate(states);
  }

  @Override
  public String getName() {
    return accumulator.getName()
        + expressions.stream()
            .map(ScalarExpression::toString)
            .collect(Collectors.joining(", ", "(", ")"));
  }

  @Override
  public String toString() {
    return getName();
  }
}
