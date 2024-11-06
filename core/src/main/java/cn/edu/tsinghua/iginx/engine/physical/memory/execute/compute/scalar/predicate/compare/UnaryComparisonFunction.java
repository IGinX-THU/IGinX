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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.SelectionBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.UnaryPredicateFunction;
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

public abstract class UnaryComparisonFunction extends UnaryPredicateFunction {

  protected UnaryComparisonFunction(String name) {
    super(name);
  }

  @Override
  public BitVector evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector input)
      throws ComputeException {
    int rowCount = getRowCount(selection, input);
    String name = getName() + "(" + input.getField().getName() + ")";
    try (BitVector dest =
        new BitVector(name, FieldType.notNullable(Types.MinorType.BIT.getType()), allocator)) {
      dest.allocateNew(rowCount);
      ConstantVectors.setValueCountWithValidity(dest, rowCount);
      ArrowBuf dataBuffer = dest.getDataBuffer();
      evaluate(
          selection,
          input,
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
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector input)
      throws ComputeException {
    int rowCount = getRowCount(selection, input);
    String name = "select_" + getName() + "(" + input.getField().getName() + ")";
    try (SelectionBuilder dest = new SelectionBuilder(allocator, name, rowCount)) {
      evaluate(
          selection,
          input,
          (index, result) -> {
            if (result) {
              dest.append(index);
            }
          });
      return dest.build();
    }
  }

  private void evaluate(
      BaseIntVector selection, FieldVector input, BiConsumer<Integer, Boolean> consumer)
      throws ComputeException {
    switch (input.getMinorType()) {
      case BIT:
        evaluate(consumer, selection, (BitVector) input);
        break;
      case INT:
        evaluate(consumer, selection, (IntVector) input);
        break;
      case BIGINT:
        evaluate(consumer, selection, (BigIntVector) input);
        break;
      case FLOAT4:
        evaluate(consumer, selection, (Float4Vector) input);
        break;
      case FLOAT8:
        evaluate(consumer, selection, (Float8Vector) input);
        break;
      case VARBINARY:
        evaluate(consumer, selection, (VarBinaryVector) input);
        break;
      default:
        throw new NotAllowTypeException(this, Schemas.of(input), 0);
    }
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer, BaseIntVector selection, BitVector input) {
    genericEvaluate(consumer, selection, input, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer, BaseIntVector selection, IntVector input) {
    genericEvaluate(consumer, selection, input, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer, BaseIntVector selection, BigIntVector input) {
    genericEvaluate(consumer, selection, input, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer, BaseIntVector selection, Float4Vector input) {
    genericEvaluate(consumer, selection, input, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer, BaseIntVector selection, Float8Vector input) {
    genericEvaluate(consumer, selection, input, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      BiConsumer<Integer, Boolean> consumer, BaseIntVector selection, VarBinaryVector input) {
    ArrowBufPointer pointer = new ArrowBufPointer();
    genericEvaluate(
        consumer,
        selection,
        input,
        index -> {
          input.getDataPointer(index, pointer);
          return evaluate(pointer);
        });
  }

  private static <T extends FieldVector> void genericEvaluate(
      BiConsumer<Integer, Boolean> consumer,
      BaseIntVector selection,
      T input,
      IntPredicate predicate) {
    int rowCount = getRowCount(selection, input);
    if (selection == null) {
      for (int i = 0; i < rowCount; i++) {
        boolean result = !input.isNull(i) && predicate.test(i);
        consumer.accept(i, result);
      }
    } else {
      for (int selectionIndex = 0; selectionIndex < rowCount; selectionIndex++) {
        int selectionValue = (int) selection.getValueAsLong(selectionIndex);
        boolean result = !input.isNull(selectionValue) && predicate.test(selectionValue);
        consumer.accept(selectionValue, result);
      }
    }
  }

  private static int getRowCount(BaseIntVector selection, FieldVector in) {
    if (selection != null) {
      return selection.getValueCount();
    }
    return in.getValueCount();
  }

  protected abstract boolean evaluate(int input);

  protected abstract boolean evaluate(long input);

  protected abstract boolean evaluate(float input);

  protected abstract boolean evaluate(double input);

  protected abstract boolean evaluate(ArrowBufPointer input);
}
