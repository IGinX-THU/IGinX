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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ExpressionAccumulator implements Accumulator {

  private final BufferAllocator allocator;
  private final Accumulator accumulator;
  private final List<ScalarExpression<?>> expressions;

  ExpressionAccumulator(
      @WillNotClose BufferAllocator allocator,
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
  public void update(@WillNotClose State state, @WillNotClose VectorSchemaRoot input)
      throws ComputeException {
    try (VectorSchemaRoot expressionResult =
        ScalarExpressions.evaluateSafe(allocator, expressions, input)) {
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
