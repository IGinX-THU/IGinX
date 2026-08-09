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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public class AndNode extends CallNode<BitVector> implements PredicateExpression {
  private final List<PredicateExpression> children;

  public AndNode(List<PredicateExpression> children) {
    this(null, children);
  }

  public AndNode(String alias, List<PredicateExpression> children) {
    super(new And(), alias, getChildren(children));
    this.children = Objects.requireNonNull(children);
  }

  private static List<ScalarExpression<BitVector>> getChildren(List<PredicateExpression> children) {
    Preconditions.checkArgument(children.size() >= 2, "AndNode should have at least 2 children");
    return Arrays.asList(
        children.get(0),
        children.subList(1, children.size()).stream()
            .reduce((a, b) -> new AndNode(Arrays.asList(a, b)))
            .orElseThrow(
                () -> new IllegalArgumentException("AndNode should have at least 2 children")));
  }

  @Override
  public String getName() {
    return children.stream().map(Objects::toString).collect(Collectors.joining(" and ", "(", ")"));
  }

  public List<PredicateExpression> getSubPredicates() {
    return Collections.unmodifiableList(children);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AndNode andNode = (AndNode) o;
    return Objects.equals(children, andNode.children);
  }

  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    return filter(allocator, dictionaryProvider, input, selection, children);
  }

  @Nullable
  private static BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection,
      List<PredicateExpression> children)
      throws ComputeException {
    try (BaseIntVector subSelection =
        children.get(0).filter(allocator, dictionaryProvider, input, selection)) {
      List<PredicateExpression> remain = children.subList(1, children.size());
      if (remain.isEmpty() || (subSelection != null && subSelection.getValueCount() == 0)) {
        return ValueVectors.slice(allocator, subSelection, "and");
      }
      return filter(allocator, dictionaryProvider, input, subSelection, remain);
    }
  }
}
