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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.CastAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.BinaryPredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.SelectionBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ArgumentException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.function.BiConsumer;
import java.util.function.IntPredicate;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

public abstract class BinaryComparisonFunction extends BinaryPredicateFunction {

  private final CastAsFloat8 castFunction = new CastAsFloat8();

  protected BinaryComparisonFunction(String name) {
    super(name);
  }

  @Override
  public BitVector evaluate(
      BufferAllocator allocator,
      @Nullable BaseIntVector selection,
      FieldVector left,
      FieldVector right)
      throws ComputeException {
    int rowCount = getRowCount(selection, left, right);
    String name =
        getName() + "(" + left.getField().getName() + "," + right.getField().getName() + ")";
    try (BitVector dest =
        new BitVector(name, FieldType.notNullable(Types.MinorType.BIT.getType()), allocator)) {
      dest.allocateNew(rowCount);
      ConstantVectors.setValueCountWithValidity(dest, rowCount);
      ArrowBuf dataBuffer = dest.getDataBuffer();
      evaluate(
          allocator,
          selection,
          left,
          right,
          (index, result) -> {
            if (result) {
              BitVectorHelper.setBit(dataBuffer, index);
            }
          });
      return ValueVectors.slice(allocator, dest);
    }
  }

  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      @Nullable BaseIntVector selection,
      FieldVector left,
      FieldVector right)
      throws ComputeException {
    int rowCount = getRowCount(selection, left, right);
    String name =
        "select_"
            + getName()
            + "("
            + left.getField().getName()
            + ","
            + right.getField().getName()
            + ")";
    try (SelectionBuilder dest = new SelectionBuilder(allocator, name, rowCount)) {
      evaluate(
          allocator,
          selection,
          left,
          right,
          (index, result) -> {
            if (result) {
              dest.append(index);
            }
          });
      return dest.build();
    }
  }

  private void evaluate(
      BufferAllocator allocator,
      BaseIntVector selection,
      FieldVector left,
      FieldVector right,
      BiConsumer<Integer, Boolean> consumer)
      throws ComputeException {
    if (left.getMinorType() == right.getMinorType()) {
      evaluateSameType(consumer, selection, left, right);
    } else if (Schemas.isNumeric(left.getMinorType()) && Schemas.isNumeric(right.getMinorType())) {
      try (FieldVector leftCast = castFunction.evaluate(allocator, left);
          FieldVector rightCast = castFunction.evaluate(allocator, right)) {
        evaluateSameType(consumer, selection, leftCast, rightCast);
      }
    } else {
      throw new ArgumentException(this, Schemas.of(left, right), "Cannot compare given types");
    }
  }

  private void evaluateSameType(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      FieldVector left,
      FieldVector right)
      throws NotAllowTypeException {
    switch (left.getMinorType()) {
      case BIT:
        evaluate(consumer, selection, (BitVector) left, (BitVector) right);
        break;
      case INT:
        evaluate(consumer, selection, (IntVector) left, (IntVector) right);
        break;
      case BIGINT:
        evaluate(consumer, selection, (BigIntVector) left, (BigIntVector) right);
        break;
      case FLOAT4:
        evaluate(consumer, selection, (Float4Vector) left, (Float4Vector) right);
        break;
      case FLOAT8:
        evaluate(consumer, selection, (Float8Vector) left, (Float8Vector) right);
        break;
      case VARBINARY:
        evaluate(consumer, selection, (VarBinaryVector) left, (VarBinaryVector) right);
        break;
      default:
        throw new NotAllowTypeException(this, Schemas.of(left, right), 0);
    }
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      BitVector left,
      BitVector right) {
    genericEvaluate(
        consumer, selection, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      IntVector left,
      IntVector right) {
    genericEvaluate(
        consumer, selection, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      BigIntVector left,
      BigIntVector right) {
    genericEvaluate(
        consumer, selection, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      Float4Vector left,
      Float4Vector right) {
    genericEvaluate(
        consumer, selection, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      Float8Vector left,
      Float8Vector right) {
    genericEvaluate(
        consumer, selection, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      VarBinaryVector left,
      VarBinaryVector right) {
    ArrowBufPointer leftPointer = new ArrowBufPointer();
    ArrowBufPointer rightPointer = new ArrowBufPointer();
    genericEvaluate(
        consumer,
        selection,
        left,
        right,
        index -> {
          left.getDataPointer(index, leftPointer);
          right.getDataPointer(index, rightPointer);
          return evaluate(leftPointer, rightPointer);
        });
  }

  private static <T extends FieldVector> void genericEvaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      T left,
      T right,
      IntPredicate predicate) {
    int rowCount = getRowCount(selection, left, right);
    if (selection == null) {
      for (int i = 0; i < rowCount; i++) {
        boolean result = !left.isNull(i) && !right.isNull(i) && predicate.test(i);
        consumer.accept(i, result);
      }
    } else {
      for (int selectionIndex = 0; selectionIndex < rowCount; selectionIndex++) {
        int selectionValue = (int) selection.getValueAsLong(selectionIndex);
        boolean result =
            !left.isNull(selectionValue)
                && !right.isNull(selectionValue)
                && predicate.test(selectionValue);
        consumer.accept(selectionValue, result);
      }
    }
  }

  private static int getRowCount(BaseIntVector selection, FieldVector left, FieldVector right) {
    if (selection != null) {
      return selection.getValueCount();
    }
    return Math.min(left.getValueCount(), right.getValueCount());
  }

  protected abstract boolean evaluate(int left, int right);

  protected abstract boolean evaluate(long left, long right);

  protected abstract boolean evaluate(float left, float right);

  protected abstract boolean evaluate(double left, double right);

  protected abstract boolean evaluate(ArrowBufPointer left, ArrowBufPointer right);
}
