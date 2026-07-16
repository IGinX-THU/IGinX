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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CompareOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public class RowCursorComparator implements Comparator<RowCursor> {

  private final CellComparator[] comparators;

  private RowCursorComparator(CellComparator[] comparators) {
    this.comparators = comparators;
  }

  @Override
  public int compare(RowCursor left, RowCursor right) {
    for (int i = 0; i < comparators.length; i++) {
      int result =
          comparators[i].compare(
              left.getColumn(i), left.getPosition(), right.getColumn(i), right.getPosition());
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  public boolean equals(RowCursor left, RowCursor right) {
    for (int i = 0; i < comparators.length; i++) {
      if (!comparators[i].equals(
          left.getColumn(i), left.getPosition(), right.getColumn(i), right.getPosition())) {
        return false;
      }
    }
    return true;
  }

  public static RowCursorComparator ofVectors(
      List<? extends ValueVector> vectors, List<CompareOption> options) {
    List<Types.MinorType> types =
        vectors.stream().map(ValueVector::getMinorType).collect(Collectors.toList());
    return ofTypes(types, options);
  }

  public static RowCursorComparator ofTypes(
      List<Types.MinorType> types, List<CompareOption> options) {
    Preconditions.checkArgument(types.size() == options.size());
    Builder builder = new Builder();
    for (int i = 0; i < types.size(); i++) {
      builder.add(types.get(i), options.get(i));
    }
    return builder.build();
  }

  public static RowCursorComparator of(VectorSchemaRoot table) {
    List<CompareOption> options =
        Stream.generate(() -> CompareOption.DEFAULT)
            .limit(table.getFieldVectors().size())
            .collect(Collectors.toList());
    return ofVectors(table.getFieldVectors(), options);
  }

  public static class Builder {
    private final List<CellComparator> comparators = new ArrayList<>();

    public Builder add(Types.MinorType type, CompareOption option) {
      CellComparator comparator = createComparator(type, option);
      comparators.add(comparator);
      return this;
    }

    public RowCursorComparator build() {
      return new RowCursorComparator(comparators.toArray(new CellComparator[0]));
    }
  }

  private interface CellComparator {
    int compare(FieldVector left, int leftIndex, FieldVector right, int rightIndex);

    boolean equals(FieldVector left, int leftIndex, FieldVector right, int rightIndex);
  }

  private static AbstractCellComparator<?> createComparator(
      Types.MinorType type, CompareOption option) {
    switch (type) {
      case BIT:
        return new BitCellComparator(option);
      case INT:
        return new IntCellComparator(option);
      case BIGINT:
        return new BigIntCellComparator(option);
      case FLOAT4:
        return new Float4Comparator(option);
      case FLOAT8:
        return new Float8Comparator(option);
      case VARBINARY:
        return new VarBinaryComparator(option);
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private abstract static class AbstractCellComparator<VEC extends FieldVector>
      implements CellComparator {

    protected final CompareOption option;

    protected AbstractCellComparator(CompareOption option) {
      this.option = option;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(FieldVector left, int leftIndex, FieldVector right, int rightIndex) {
      if (left.isNull(leftIndex)) {
        if (right.isNull(rightIndex)) {
          return 0;
        }
        return option.isNullLast() ? 1 : -1;
      }
      if (right.isNull(rightIndex)) {
        return option.isNullLast() ? -1 : 1;
      }
      int result = compareNonnull((VEC) left, leftIndex, (VEC) right, rightIndex);
      return option.isDescending() ? -result : result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(FieldVector left, int leftIndex, FieldVector right, int rightIndex) {
      if (left.isNull(leftIndex)) {
        return right.isNull(rightIndex);
      }
      if (right.isNull(rightIndex)) {
        return false;
      }
      return equalsNonnull((VEC) left, leftIndex, (VEC) right, rightIndex);
    }

    protected abstract int compareNonnull(VEC left, int leftIndex, VEC right, int rightIndex);

    protected abstract boolean equalsNonnull(VEC left, int leftIndex, VEC right, int rightIndex);
  }

  private static class BitCellComparator extends AbstractCellComparator<BitVector> {

    protected BitCellComparator(CompareOption option) {
      super(option);
    }

    @Override
    protected int compareNonnull(BitVector left, int leftIndex, BitVector right, int rightIndex) {
      return Integer.compare(left.get(leftIndex), right.get(rightIndex));
    }

    @Override
    protected boolean equalsNonnull(
        BitVector left, int leftIndex, BitVector right, int rightIndex) {
      return left.get(leftIndex) == right.get(rightIndex);
    }
  }

  private static class IntCellComparator extends AbstractCellComparator<IntVector> {

    protected IntCellComparator(CompareOption option) {
      super(option);
    }

    @Override
    protected int compareNonnull(IntVector left, int leftIndex, IntVector right, int rightIndex) {
      return Integer.compare(left.get(leftIndex), right.get(rightIndex));
    }

    @Override
    protected boolean equalsNonnull(
        IntVector left, int leftIndex, IntVector right, int rightIndex) {
      return left.get(leftIndex) == right.get(rightIndex);
    }
  }

  private static class BigIntCellComparator extends AbstractCellComparator<BigIntVector> {

    protected BigIntCellComparator(CompareOption option) {
      super(option);
    }

    @Override
    protected int compareNonnull(
        BigIntVector left, int leftIndex, BigIntVector right, int rightIndex) {
      return Long.compare(left.get(leftIndex), right.get(rightIndex));
    }

    @Override
    protected boolean equalsNonnull(
        BigIntVector left, int leftIndex, BigIntVector right, int rightIndex) {
      return left.get(leftIndex) == right.get(rightIndex);
    }
  }

  private static class Float4Comparator extends AbstractCellComparator<Float4Vector> {

    protected Float4Comparator(CompareOption option) {
      super(option);
    }

    @Override
    protected int compareNonnull(
        Float4Vector left, int leftIndex, Float4Vector right, int rightIndex) {
      return Float.compare(left.get(leftIndex), right.get(rightIndex));
    }

    @Override
    protected boolean equalsNonnull(
        Float4Vector left, int leftIndex, Float4Vector right, int rightIndex) {
      return left.get(leftIndex) == right.get(rightIndex);
    }
  }

  private static class Float8Comparator extends AbstractCellComparator<Float8Vector> {

    protected Float8Comparator(CompareOption option) {
      super(option);
    }

    @Override
    protected int compareNonnull(
        Float8Vector left, int leftIndex, Float8Vector right, int rightIndex) {
      return Double.compare(left.get(leftIndex), right.get(rightIndex));
    }

    @Override
    protected boolean equalsNonnull(
        Float8Vector left, int leftIndex, Float8Vector right, int rightIndex) {
      return left.get(leftIndex) == right.get(rightIndex);
    }
  }

  private static class VarBinaryComparator extends AbstractCellComparator<VarBinaryVector> {

    protected VarBinaryComparator(CompareOption option) {
      super(option);
    }

    @Override
    protected int compareNonnull(
        VarBinaryVector left, int leftIndex, VarBinaryVector right, int rightIndex) {
      ArrowBufPointer leftPointer = left.getDataPointer(leftIndex);
      ArrowBufPointer rightPointer = right.getDataPointer(rightIndex);
      return leftPointer.compareTo(rightPointer);
    }

    @Override
    protected boolean equalsNonnull(
        VarBinaryVector left, int leftIndex, VarBinaryVector right, int rightIndex) {
      ArrowBufPointer leftPointer = left.getDataPointer(leftIndex);
      ArrowBufPointer rightPointer = right.getDataPointer(rightIndex);
      return leftPointer.equals(rightPointer);
    }
  }
}
