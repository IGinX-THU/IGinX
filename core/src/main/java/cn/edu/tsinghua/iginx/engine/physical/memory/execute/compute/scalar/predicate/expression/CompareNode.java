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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.PredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
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
import org.apache.arrow.vector.dictionary.DictionaryProvider;

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

  @Override
  public String getName() {
    return function.getName()
        + children.stream().map(Objects::toString).collect(Collectors.joining(",", "(", ")"));
  }

  @Nullable
  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    if (children.stream().allMatch(e -> e instanceof LiteralNode || e instanceof FieldNode)) {
      try (VectorSchemaRoot args =
          ScalarExpressionUtils.evaluate(allocator, dictionaryProvider, input, null, children)) {
        return function.filter(allocator, dictionaryProvider, args, selection);
      }
    }
    try (VectorSchemaRoot args =
        ScalarExpressionUtils.evaluate(allocator, dictionaryProvider, input, selection, children)) {
      try (BaseIntVector result = function.filter(allocator, dictionaryProvider, args, null)) {
        if (selection == null) {
          return ValueVectors.slice(allocator, result);
        }
        return PhysicalFunctions.take(allocator, result, selection);
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != getClass()) return false;
    CompareNode compareNode = (CompareNode) obj;
    return function.equals(compareNode.function) && getChildren().equals(compareNode.getChildren());
  }
}
