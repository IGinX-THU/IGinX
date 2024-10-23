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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.Cast;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArgumentException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowTypeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.function.IntPredicate;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public abstract class ComparisonFunction extends BinaryFunction {

  protected ComparisonFunction(String name) {
    super(name);
  }

  @Override
  public BitVector evaluate(
      @WillNotClose BufferAllocator allocator,
      @WillNotClose FieldVector left,
      @WillNotClose FieldVector right)
      throws ComputeException {
    if (left.getMinorType() == right.getMinorType()) {
      return evaluateSameType(allocator, left, right);
    }
    if (Schemas.isNumeric(left.getMinorType()) && Schemas.isNumeric(right.getMinorType())) {
      Cast castFunction = new Cast(Types.MinorType.FLOAT8);
      try (FieldVector leftCast = castFunction.evaluate(allocator, left);
          FieldVector rightCast = castFunction.evaluate(allocator, right)) {
        return evaluateSameType(allocator, leftCast, rightCast);
      }
    }
    throw new ArgumentException(this, Schemas.of(left, right), "Cannot compare given types");
  }

  private BitVector evaluateSameType(BufferAllocator allocator, FieldVector left, FieldVector right)
      throws NotAllowTypeException {
    int rowCount = Math.min(left.getValueCount(), right.getValueCount());
    BitVector dest = (BitVector) ValueVectors.create(allocator, Types.MinorType.BIT, rowCount);
    if (left instanceof NullVector || right instanceof NullVector) {
      return dest;
    }

    switch (left.getMinorType()) {
      case INT:
        evaluate(dest, (IntVector) left, (IntVector) right);
        break;
      case BIGINT:
        evaluate(dest, (BigIntVector) left, (BigIntVector) right);
        break;
      case FLOAT4:
        evaluate(dest, (Float4Vector) left, (Float4Vector) right);
        break;
      case FLOAT8:
        evaluate(dest, (Float8Vector) left, (Float8Vector) right);
        break;
      case VARBINARY:
        evaluate(dest, (VarBinaryVector) left, (VarBinaryVector) right);
      default:
        dest.close();
        throw new NotAllowTypeException(this, Schemas.of(left, right), 0);
    }
    return dest;
  }

  private void evaluate(BitVector dest, IntVector left, IntVector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, BigIntVector left, BigIntVector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, Float4Vector left, Float4Vector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, Float8Vector left, Float8Vector right) {
    genericEvaluate(dest, left, right, index -> evaluate(left.get(index), right.get(index)));
  }

  private void evaluate(BitVector dest, VarBinaryVector left, VarBinaryVector right) {
    ArrowBufPointer leftPointer = new ArrowBufPointer();
    ArrowBufPointer rightPointer = new ArrowBufPointer();
    genericEvaluate(
        dest,
        left,
        right,
        index -> {
          left.getDataPointer(index, leftPointer);
          right.getDataPointer(index, rightPointer);
          return evaluate(leftPointer, rightPointer);
        });
  }

  // TODO: 可以和 arithmetic 进行代码复用
  private <T extends FieldVector> void genericEvaluate(
      BitVector dest, T left, T right, IntPredicate predicate) {
    int rowCount = dest.getValueCount();

    new And()
        .evaluate(
            dest.getValidityBuffer(),
            left.getValidityBuffer(),
            right.getValidityBuffer(),
            BitVectorHelper.getValidityBufferSize(dest.getValueCount()));

    ArrowBuf destDataBuf = dest.getDataBuffer();
    for (int i = 0; i < rowCount; i++) {
      if (!dest.isNull(i) && predicate.test(i)) {
        BitVectorHelper.setBit(destDataBuf, i);
      }
    }
  }

  protected abstract boolean evaluate(int left, int right);

  protected abstract boolean evaluate(long left, long right);

  protected abstract boolean evaluate(float left, float right);

  protected abstract boolean evaluate(double left, double right);

  protected abstract boolean evaluate(ArrowBufPointer left, ArrowBufPointer right);
}
