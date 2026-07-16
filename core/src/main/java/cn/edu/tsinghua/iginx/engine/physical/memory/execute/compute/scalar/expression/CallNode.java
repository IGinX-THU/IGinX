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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * CallNode：用于调用函数的节点
 *
 * @param <OUTPUT>
 */
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
    if (obj == this) return true;
    if (obj == null || obj.getClass() != getClass()) return false;
    CallNode<?> callNode = (CallNode<?>) obj;
    return function.equals(callNode.function) && getChildren().equals(callNode.getChildren());
  }

  @Override
  public Set<ScalarExpression<?>> getLeafExpressions() {
    return getChildren().stream()
        .map(ScalarExpression::getLeafExpressions)
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
  }

  @Override
  protected OUTPUT invokeImpl(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException {
    if (getChildren().stream()
        .allMatch(child -> child instanceof LiteralNode || child instanceof FieldNode)) {
      try (VectorSchemaRoot args =
          ScalarExpressionUtils.evaluate(
              allocator, dictionaryProvider, input, null, getChildren())) {
        return function.invoke(allocator, dictionaryProvider, selection, args);
      }
    }
    try (VectorSchemaRoot args =
        ScalarExpressionUtils.evaluate(
            allocator, dictionaryProvider, input, selection, getChildren())) {
      return function.invoke(allocator, dictionaryProvider, null, args);
    }
  }
}
