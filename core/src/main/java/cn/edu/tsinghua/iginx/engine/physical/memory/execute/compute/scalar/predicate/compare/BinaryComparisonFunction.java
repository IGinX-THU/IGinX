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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.CastAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.BinaryPredicateFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.*;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ArgumentException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import java.util.Collections;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
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
          ArrowDictionaries.emptyProvider(),
          selection,
          left,
          right,
          rowCount,
          (index, result) -> {
            if (result) {
              BitVectorHelper.setBit(dataBuffer, index);
            }
          });
      return ValueVectors.slice(allocator, dest);
      // 考虑字典本身为 null 的情况
    }
  }

  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      FieldVector left,
      FieldVector right,
      @Nullable BaseIntVector selection)
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
          dictionaryProvider,
          selection,
          left,
          right,
          rowCount,
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
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      FieldVector left,
      FieldVector right,
      int rowCount,
      IntBooleanConsumer consumer)
      throws ComputeException {
    Field leftFlattenedType = ArrowDictionaries.flatten(dictionaryProvider, left.getField());
    Field rightFlattenedType = ArrowDictionaries.flatten(dictionaryProvider, right.getField());
    Types.MinorType leftMinorType = Types.getMinorTypeForArrowType(leftFlattenedType.getType());
    Types.MinorType rightMinorType = Types.getMinorTypeForArrowType(rightFlattenedType.getType());

    if (Objects.equals(leftMinorType, rightMinorType)) {
      evaluateSameType(dictionaryProvider, left, right, rowCount, selection, consumer);
    } else if (Schemas.isNumeric(left.getMinorType()) && Schemas.isNumeric(right.getMinorType())) {
      // TODO: cast only selected indices
      try (FieldVector leftCast =
              castFunction.invoke(
                  allocator,
                  dictionaryProvider,
                  selection,
                  new VectorSchemaRoot(Collections.singleton(left)));
          FieldVector rightCast =
              castFunction.invoke(
                  allocator,
                  dictionaryProvider,
                  selection,
                  new VectorSchemaRoot(Collections.singleton(right)))) {
        evaluateSameType(
            dictionaryProvider,
            leftCast,
            rightCast,
            rowCount,
            null,
            (index, matched) -> {
              consumer.accept(
                  selection == null ? index : (int) selection.getValueAsLong(index), matched);
            });
      }
    } else {
      throw new ArgumentException(
          this, Schemas.of(leftFlattenedType, rightFlattenedType), "Cannot compare given types");
    }
  }

  private void evaluateSameType(
      @Nullable DictionaryProvider dictionaryProvider,
      FieldVector left,
      FieldVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      IntBooleanConsumer consumer)
      throws NotAllowTypeException {
    if (dictionaryProvider == null) {
      evaluateSameType(left, right, rowCount, selection, null, null, consumer);
      return;
    }
    DictionaryEncoding leftDictionaryEncoding = left.getField().getDictionary();
    if (leftDictionaryEncoding == null) {
      evaluateSameType(dictionaryProvider, left, right, rowCount, selection, null, consumer);
      return;
    }
    Dictionary leftDictionary = dictionaryProvider.lookup(leftDictionaryEncoding.getId());
    evaluateSameType(
        dictionaryProvider,
        leftDictionary.getVector(),
        right,
        rowCount,
        selection,
        (BaseIntVector) left,
        consumer);
  }

  private void evaluateSameType(
      DictionaryProvider dictionaryProvider,
      FieldVector left,
      FieldVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      IntBooleanConsumer consumer)
      throws NotAllowTypeException {
    DictionaryEncoding rightDictionaryEncoding = right.getField().getDictionary();
    if (rightDictionaryEncoding == null) {
      evaluateSameType(left, right, rowCount, selection, leftIndices, null, consumer);
      return;
    }
    Dictionary rightDictionary = dictionaryProvider.lookup(rightDictionaryEncoding.getId());
    evaluateSameType(
        left,
        rightDictionary.getVector(),
        rowCount,
        selection,
        leftIndices,
        (BaseIntVector) right,
        consumer);
  }

  private void evaluateSameType(
      FieldVector left,
      FieldVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer)
      throws NotAllowTypeException {
    switch (left.getMinorType()) {
      case BIT:
        evaluate(
            (BitVector) left,
            (BitVector) right,
            rowCount,
            selection,
            leftIndices,
            rightIndices,
            consumer);
        break;
      case INT:
        evaluate(
            (IntVector) left,
            (IntVector) right,
            rowCount,
            selection,
            leftIndices,
            rightIndices,
            consumer);
        break;
      case BIGINT:
        evaluate(
            (BigIntVector) left,
            (BigIntVector) right,
            rowCount,
            selection,
            leftIndices,
            rightIndices,
            consumer);
        break;
      case FLOAT4:
        evaluate(
            (Float4Vector) left,
            (Float4Vector) right,
            rowCount,
            selection,
            leftIndices,
            rightIndices,
            consumer);
        break;
      case FLOAT8:
        evaluate(
            (Float8Vector) left,
            (Float8Vector) right,
            rowCount,
            selection,
            leftIndices,
            rightIndices,
            consumer);
        break;
      case VARBINARY:
        evaluate(
            (VarBinaryVector) left,
            (VarBinaryVector) right,
            rowCount,
            selection,
            leftIndices,
            rightIndices,
            consumer);
        break;
      default:
        throw new NotAllowTypeException(this, Schemas.of(left, right), 0);
    }
  }

  private void evaluate(
      BitVector left,
      BitVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer) {
    genericEvaluate(
        left,
        right,
        rowCount,
        selection,
        leftIndices,
        rightIndices,
        consumer,
        (l, r) -> evaluate(left.get(l), right.get(r)));
  }

  private void evaluate(
      IntVector left,
      IntVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer) {
    genericEvaluate(
        left,
        right,
        rowCount,
        selection,
        leftIndices,
        rightIndices,
        consumer,
        (l, r) -> evaluate(left.get(l), right.get(r)));
  }

  private void evaluate(
      BigIntVector left,
      BigIntVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer) {
    genericEvaluate(
        left,
        right,
        rowCount,
        selection,
        leftIndices,
        rightIndices,
        consumer,
        (l, r) -> evaluate(left.get(l), right.get(r)));
  }

  private void evaluate(
      Float4Vector left,
      Float4Vector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer) {
    genericEvaluate(
        left,
        right,
        rowCount,
        selection,
        leftIndices,
        rightIndices,
        consumer,
        (l, r) -> evaluate(left.get(l), right.get(r)));
  }

  private void evaluate(
      Float8Vector left,
      Float8Vector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer) {
    genericEvaluate(
        left,
        right,
        rowCount,
        selection,
        leftIndices,
        rightIndices,
        consumer,
        (l, r) -> evaluate(left.get(l), right.get(r)));
  }

  private void evaluate(
      VarBinaryVector left,
      VarBinaryVector right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer) {
    ArrowBufPointer leftPointer = new ArrowBufPointer();
    ArrowBufPointer rightPointer = new ArrowBufPointer();
    genericEvaluate(
        left,
        right,
        rowCount,
        selection,
        leftIndices,
        rightIndices,
        consumer,
        (l, r) -> {
          left.getDataPointer(l, leftPointer);
          right.getDataPointer(r, rightPointer);
          return evaluate(leftPointer, rightPointer);
        });
  }

  private static <T extends FieldVector> void genericEvaluate(
      T left,
      T right,
      int rowCount,
      @Nullable BaseIntVector selection,
      @Nullable BaseIntVector leftIndices,
      @Nullable BaseIntVector rightIndices,
      IntBooleanConsumer consumer,
      IntIntPredicate predicate) {

    for (int resultIndex = 0; resultIndex < rowCount; resultIndex++) {
      int selectedIndex = resultIndex;
      if (selection != null) {
        selectedIndex = (int) selection.getValueAsLong(resultIndex);
      }
      int leftSelectedIndex = selectedIndex;
      if (leftIndices != null) {
        if (leftIndices.isNull(selectedIndex)) {
          consumer.accept(selectedIndex, false);
          continue;
        }
        leftSelectedIndex = (int) leftIndices.getValueAsLong(selectedIndex);
      }
      int rightSelectedIndex = selectedIndex;
      if (rightIndices != null) {
        if (rightIndices.isNull(selectedIndex)) {
          consumer.accept(selectedIndex, false);
          continue;
        }
        rightSelectedIndex = (int) rightIndices.getValueAsLong(selectedIndex);
      }
      boolean result =
          !left.isNull(leftSelectedIndex)
              && !right.isNull(rightSelectedIndex)
              && predicate.test(leftSelectedIndex, rightSelectedIndex);
      consumer.accept(selectedIndex, result);
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
