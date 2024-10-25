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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.sort;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.AbstractFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CompareOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursorComparator;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class IndexSort extends AbstractFunction<IntVector> {

  private final List<CompareOption> options;

  public IndexSort(List<CompareOption> options) {
    super(createName(options), new Arity(options.size()));
    this.options = options;
  }

  private static String createName(List<CompareOption> options) {
    return "sort"
        + options.stream().map(CompareOption::toString).collect(Collectors.joining(", ", "<", ">"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IndexSort)) return false;
    IndexSort indexSort = (IndexSort) o;
    return Objects.equals(options, indexSort.options);
  }

  @Override
  protected IntVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    Integer[] indices = getIndices(input.getRowCount());
    Comparator<Integer> comparator = getComparator(input);
    for (int i = 0; i < indices.length; i++) {
      indices[i] = i;
    }
    Arrays.sort(indices, comparator);
    return ValueVectors.ofNonnull(allocator, "sorted_indices", indices);
  }

  private static Integer[] getIndices(int size) {
    Integer[] indices = new Integer[size];
    for (int i = 0; i < size; i++) {
      indices[i] = i;
    }
    return indices;
  }

  private Comparator<Integer> getComparator(VectorSchemaRoot input) throws ComputeException {
    RowCursor leftCursor = new RowCursor(input);
    RowCursor rightCursor = new RowCursor(input);
    RowCursorComparator comparator = RowCursorComparator.ofVectors(input.getFieldVectors(), options);

    return (left, right) -> {
      leftCursor.setPosition(left);
      rightCursor.setPosition(right);
      return comparator.compare(leftCursor, rightCursor);
    };
  }
}
