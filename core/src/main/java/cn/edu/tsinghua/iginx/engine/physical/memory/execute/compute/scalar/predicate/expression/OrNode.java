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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.Or;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.SelectionBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public class OrNode extends CallNode<BitVector> implements PredicateExpression {
  private final List<PredicateExpression> children;

  public static OrNode createFalse() {
    return new OrNode(Collections.emptyList());
  }

  public OrNode(List<PredicateExpression> children) {
    this(null, children);
  }

  public OrNode(String alias, List<PredicateExpression> children) {
    super(new Or(), alias, getChildren(children));

    this.children = Objects.requireNonNull(children);
  }

  private static List<ScalarExpression<BitVector>> getChildren(List<PredicateExpression> children) {
    Preconditions.checkArgument(children.size() >= 2, "OrNode should have at least 2 children");
    return Arrays.asList(
        children.get(0),
        children.subList(1, children.size()).stream()
            .reduce((a, b) -> new OrNode(Arrays.asList(a, b)))
            .orElseThrow(
                () -> new IllegalArgumentException("AndNode should have at least 2 children")));
  }

  @Override
  public String getName() {
    return children.stream().map(Objects::toString).collect(Collectors.joining(" and ", "(", ")"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OrNode orNode = (OrNode) o;
    return Objects.equals(children, orNode.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), children);
  }

  public List<PredicateExpression> getSubPredicates() {
    return Collections.unmodifiableList(children);
  }

  @Nullable
  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    Set<Integer> remainIndex = new HashSet<>();
    if (selection == null) {
      IntStream.range(0, input.getRowCount()).forEach(remainIndex::add);
    } else {
      for (int i = 0; i < selection.getValueCount(); i++) {
        remainIndex.add((int) selection.getValueAsLong(i));
      }
    }
    try (SelectionBuilder selectionBuilder =
        new SelectionBuilder(allocator, "or", input.getRowCount())) {
      return filter(
          allocator, dictionaryProvider, input, selection, children, selectionBuilder, remainIndex);
    }
  }

  private static BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection,
      List<PredicateExpression> children,
      SelectionBuilder resultBuilder,
      Set<Integer> remainIndex)
      throws ComputeException {
    try (BaseIntVector subSelection =
        children.get(0).filter(allocator, dictionaryProvider, input, selection)) {
      if (subSelection == null) {
        return ValueVectors.slice(allocator, selection, "or");
      }
      for (int i = 0; i < subSelection.getValueCount(); i++) {
        int index = (int) subSelection.getValueAsLong(i);
        resultBuilder.append(index);
        remainIndex.remove(index);
      }
      if (remainIndex.isEmpty()) {
        return ValueVectors.slice(allocator, selection, "or");
      }
      List<PredicateExpression> remainChildren = children.subList(1, children.size());
      if (remainChildren.isEmpty()) {
        return resultBuilder.build();
      }
      if (subSelection.getValueCount() == 0) {
        return filter(
            allocator,
            dictionaryProvider,
            input,
            selection,
            remainChildren,
            resultBuilder,
            remainIndex);
      }
      try (SelectionBuilder remainSelectionBuilder =
          new SelectionBuilder(allocator, "or", remainIndex.size())) {
        for (int index : remainIndex) {
          remainSelectionBuilder.append(index);
        }
        try (BaseIntVector remainSelection = remainSelectionBuilder.build()) {
          return filter(
              allocator,
              dictionaryProvider,
              input,
              remainSelection,
              remainChildren,
              resultBuilder,
              remainIndex);
        }
      }
    }
  }
}
