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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class CallNode<OUTPUT extends FieldVector> extends AbstractScalarExpression<OUTPUT> {

  private final ScalarFunction<OUTPUT> function;

  public CallNode(ScalarFunction<OUTPUT> function, ScalarExpression<?>... children) {
    this(function, null, children);
  }

  public CallNode(ScalarFunction<OUTPUT> function, List<? extends ScalarExpression<?>> children) {
    this(function, null, children);
  }

  public CallNode(
      ScalarFunction<OUTPUT> function, @Nullable String alias, ScalarExpression<?>... children) {
    this(function, alias, Arrays.asList(children));
  }

  public CallNode(
      ScalarFunction<OUTPUT> function,
      @Nullable String alias,
      List<? extends ScalarExpression<?>> children) {
    super(alias, children);
    this.function = Preconditions.checkNotNull(function);
  }

  @Override
  public String getName() {
    return function.getName()
        + getChildren().stream()
            .map(ScalarExpression::toString)
            .collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CallNode) {
      CallNode<?> callNode = (CallNode<?>) obj;
      return function.equals(callNode.function) && getChildren().equals(callNode.getChildren());
    }
    return false;
  }

  @Override
  protected OUTPUT invokeImpl(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (getChildren().stream().allMatch(e -> e instanceof LiteralNode || e instanceof FieldNode)) {
      try (VectorSchemaRoot args =
          ScalarExpressions.evaluateSafe(allocator, getChildren(), input)) {
        return function.invoke(allocator, selection, args);
      }
    }
    try (VectorSchemaRoot args =
        ScalarExpressions.evaluateSafe(allocator, getChildren(), selection, input)) {
      return function.invoke(allocator, args);
    }
  }
}
