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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.PredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class CompareNode extends CallNode<BitVector> implements PredicateExpression {

  private final PredicateFunction function;
  private final List<ScalarExpression<?>> children;

  public CompareNode(PredicateFunction function, ScalarExpression<?>... children) {
    this(function, null, Arrays.asList(children));
  }

  public CompareNode(PredicateFunction function, String alias, List<ScalarExpression<?>> children) {
    super(function, alias, children);
    this.function = Objects.requireNonNull(function);
    this.children = Objects.requireNonNull(children);
  }

  public CompareNode(PredicateFunction function, List<ScalarExpression<?>> children) {
    this(function, null, children);
  }

  @Override
  public String getName() {
    return function.getName()
        + children.stream().map(Objects::toString).collect(Collectors.joining(",", "(", ")"));
  }

  @Nullable
  @Override
  public BaseIntVector filter(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (children.stream().allMatch(e -> e instanceof LiteralNode || e instanceof FieldNode)) {
      try (VectorSchemaRoot args = ScalarExpressions.evaluateSafe(allocator, children, input)) {
        return function.filter(allocator, selection, args);
      }
    }
    try (VectorSchemaRoot args =
        ScalarExpressions.evaluateSafe(allocator, children, selection, input)) {
      return function.filter(allocator, null, args);
    }
  }
}
