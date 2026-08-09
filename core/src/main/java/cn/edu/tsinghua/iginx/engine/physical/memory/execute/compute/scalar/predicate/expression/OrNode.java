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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.Or;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.SelectionBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import io.netty.util.collection.IntObjectHashMap;
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
    return children.stream().map(Objects::toString).collect(Collectors.joining(" or ", "(", ")"));
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
        if (selection.getField().isNullable() && selection.isNull(i)) {
          continue;
        }
        remainIndex.add((int) selection.getValueAsLong(i));
      }
    }

    ResultBuilder resultBuilder = new ResultBuilder(allocator, selection);
    BaseIntVector result =
        filter(
            allocator, dictionaryProvider, input, selection, children, resultBuilder, remainIndex);
    if (result != null) {
      return result;
    }
    return ValueVectors.slice(allocator, selection, "or");
  }

  @Nullable
  private static BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection,
      List<PredicateExpression> children,
      ResultBuilder resultBuilder,
      Set<Integer> remainIndex)
      throws ComputeException {
    try (BaseIntVector subSelection =
        children.get(0).filter(allocator, dictionaryProvider, input, selection)) {
      if (subSelection == null) {
        return null;
      }
      for (int i = 0; i < subSelection.getValueCount(); i++) {
        int index = (int) subSelection.getValueAsLong(i);
        resultBuilder.append(index);
        remainIndex.remove(index);
      }
      if (remainIndex.isEmpty()) {
        return null;
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

  private static class ResultBuilder {
    private final BufferAllocator allocator;
    @Nullable private final IntObjectHashMap<Integer> invertedIndex;
    private final List<Integer> result = new ArrayList<>();

    public ResultBuilder(BufferAllocator allocator, BaseIntVector selection) {
      this.allocator = Objects.requireNonNull(allocator);
      if (selection == null) {
        invertedIndex = null;
      } else {
        invertedIndex = new IntObjectHashMap<>();
        for (int i = 0; i < selection.getValueCount(); i++) {
          if (selection.getField().isNullable() && selection.isNull(i)) {
            continue;
          }
          int value = (int) selection.getValueAsLong(i);
          invertedIndex.put(value, Integer.valueOf(i));
        }
      }
    }

    public void append(int index) {
      result.add(index);
    }

    public BaseIntVector build() {
      if (invertedIndex == null) {
        result.sort(Comparator.naturalOrder());
      } else {
        result.sort(Comparator.comparingInt(invertedIndex::get));
      }
      try (SelectionBuilder selectionBuilder =
          new SelectionBuilder(allocator, "or", result.size())) {
        result.forEach(selectionBuilder::append);
        return selectionBuilder.build();
      }
    }
  }
}
