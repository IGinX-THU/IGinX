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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.UnaryPredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.SelectionBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import java.util.function.BiConsumer;
import java.util.function.IntPredicate;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
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
          null,
          input,
          rowCount,
          selection,
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
      DictionaryProvider dictionaryProvider,
      FieldVector input,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    int rowCount = getRowCount(selection, input);
    String name = "select_" + getName() + "(" + input.getField().getName() + ")";
    try (SelectionBuilder dest = new SelectionBuilder(allocator, name, rowCount)) {
      evaluate(
          dictionaryProvider,
          input,
          rowCount,
          selection,
          (index, result) -> {
            if (result) {
              dest.append(index);
            }
          });
      return dest.build();
    }
  }

  private void evaluate(
      @Nullable DictionaryProvider dictionaryProvider,
      FieldVector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      BiConsumer<Integer, Boolean> consumer)
      throws ComputeException {
    if (dictionaryProvider == null) {
      evaluate(input, rowCount, selection, null, consumer);
    } else {
      DictionaryEncoding encoding = input.getField().getDictionary();
      if (encoding == null) {
        evaluate(input, rowCount, selection, null, consumer);
      } else {
        Dictionary dictionary = dictionaryProvider.lookup(encoding.getId());
        evaluate(dictionary.getVector(), rowCount, selection, (BaseIntVector) input, consumer);
      }
    }
  }

  private void evaluate(
      FieldVector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer)
      throws ComputeException {
    switch (input.getMinorType()) {
      case BIT:
        evaluate((BitVector) input, rowCount, selection, indices, consumer);
        break;
      case INT:
        evaluate((IntVector) input, rowCount, selection, indices, consumer);
        break;
      case BIGINT:
        evaluate((BigIntVector) input, rowCount, selection, indices, consumer);
        break;
      case FLOAT4:
        evaluate((Float4Vector) input, rowCount, selection, indices, consumer);
        break;
      case FLOAT8:
        evaluate((Float8Vector) input, rowCount, selection, indices, consumer);
        break;
      case VARBINARY:
        evaluate((VarBinaryVector) input, rowCount, selection, indices, consumer);
        break;
      default:
        throw new NotAllowTypeException(this, Schemas.of(input), 0);
    }
  }

  private void evaluate(
      BitVector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer) {
    genericEvaluate(
        input, rowCount, selection, indices, consumer, index -> evaluate(input.get(index) != 0));
  }

  private void evaluate(
      IntVector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer) {
    genericEvaluate(
        input, rowCount, selection, indices, consumer, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      BigIntVector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer) {
    genericEvaluate(
        input, rowCount, selection, indices, consumer, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      Float4Vector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer) {
    genericEvaluate(
        input, rowCount, selection, indices, consumer, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      Float8Vector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer) {
    genericEvaluate(
        input, rowCount, selection, indices, consumer, index -> evaluate(input.get(index)));
  }

  private void evaluate(
      VarBinaryVector input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer) {
    ArrowBufPointer pointer = new ArrowBufPointer();
    genericEvaluate(
        input,
        rowCount,
        selection,
        indices,
        consumer,
        index -> {
          input.getDataPointer(index, pointer);
          return evaluate(pointer);
        });
  }

  private static <T extends FieldVector> void genericEvaluate(
      T input,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector indices,
      BiConsumer<Integer, Boolean> consumer,
      IntPredicate predicate) {
    for (int resultIndex = 0; resultIndex < rowCount; resultIndex++) {
      int selectedIndex = resultIndex;
      if (selection != null) {
        selectedIndex = (int) selection.getValueAsLong(resultIndex);
      }
      int index = selectedIndex;
      if (indices != null) {
        if (indices.isNull(selectedIndex)) {
          consumer.accept(selectedIndex, false);
          continue;
        }
        index = (int) indices.getValueAsLong(selectedIndex);
      }
      boolean result = !input.isNull(index) && predicate.test(index);
      consumer.accept(selectedIndex, result);
    }
  }

  private static int getRowCount(BaseIntVector selection, FieldVector in) {
    if (selection != null) {
      return selection.getValueCount();
    }
    return in.getValueCount();
  }

  protected abstract boolean evaluate(boolean input);

  protected abstract boolean evaluate(int input);

  protected abstract boolean evaluate(long input);

  protected abstract boolean evaluate(float input);

  protected abstract boolean evaluate(double input);

  protected abstract boolean evaluate(ArrowBufPointer input);
}
