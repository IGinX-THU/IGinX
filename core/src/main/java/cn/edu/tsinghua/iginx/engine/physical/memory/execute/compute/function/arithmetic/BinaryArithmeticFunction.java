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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.BinaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.cast.CastNumericAsFloat8;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.And;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public abstract class BinaryArithmeticFunction extends BinaryFunction<FieldVector> {

  protected final CastNumericAsFloat8 castFunction = new CastNumericAsFloat8();

  protected BinaryArithmeticFunction(String name) {
    super(name);
  }

  @Override
  protected boolean allowLeftType(Types.MinorType type) {
    return Schemas.isNumeric(type);
  }

  @Override
  protected boolean allowRightType(Types.MinorType type) {
    return Schemas.isNumeric(type);
  }

  @Override
  public FieldVector evaluate(ExecutorContext context, FieldVector left, FieldVector right) {
    if (left instanceof NullVector || right instanceof NullVector) {
      return ConstantVectors.ofNull(context.getAllocator(), left.getValueCount());
    }
    if (left.getMinorType() == right.getMinorType()) {
      return evaluateSameType(context, left, right);
    } else {
      try (FieldVector leftCast = castFunction.evaluate(context, left);
           FieldVector rightCast = castFunction.evaluate(context, right)) {
        return evaluateSameType(context, leftCast, rightCast);
      }
    }
  }

  public FieldVector evaluateSameType(ExecutorContext context, FieldVector left, FieldVector right) {
    FieldVector dest = ValueVectors.likeWithBothValidity(context.getAllocator(), left, right);
    switch (left.getMinorType()) {
      case INT:
        evaluate((IntVector) dest, (IntVector) left, (IntVector) right);
        break;
      case BIGINT:
        evaluate((BigIntVector) dest, (BigIntVector) left, (BigIntVector) right);
        break;
      case FLOAT4:
        evaluate((Float4Vector) dest, (Float4Vector) left, (Float4Vector) right);
        break;
      case FLOAT8:
        evaluate((Float8Vector) dest, (Float8Vector) left, (Float8Vector) right);
        break;
      default:
        throw new IllegalStateException("Unsupported type: " + left.getMinorType());
    }
    return dest;
  }

  public void evaluate(IntVector dest, IntVector left, IntVector right) {
    checkCapacity(dest, left, right);
    andValidity(dest, left, right);
    int rowCount = dest.getValueCount();
    ArrowBuf destBuf = dest.getDataBuffer();
    for (int i = 0; i < rowCount; i++) {
      long index = i * (long) IntVector.TYPE_WIDTH;
      destBuf.setInt(index, evaluate(left.get(i), right.get(i)));
    }
  }

  public void evaluate(BigIntVector dest, BigIntVector left, BigIntVector right) {
    checkCapacity(dest, left, right);
    andValidity(dest, left, right);
    int rowCount = dest.getValueCount();
    ArrowBuf destBuf = dest.getDataBuffer();
    for (int i = 0; i < rowCount; i++) {
      long index = i * (long) BigIntVector.TYPE_WIDTH;
      destBuf.setLong(index, evaluate(left.get(i), right.get(i)));
    }
  }

  public void evaluate(Float4Vector dest, Float4Vector left, Float4Vector right) {
    checkCapacity(dest, left, right);
    andValidity(dest, left, right);
    int rowCount = dest.getValueCount();
    ArrowBuf destBuf = dest.getDataBuffer();
    for (int i = 0; i < rowCount; i++) {
      long index = i * (long) Float4Vector.TYPE_WIDTH;
      destBuf.setFloat(index, evaluate(left.get(i), right.get(i)));
    }
  }

  public void evaluate(Float8Vector dest, Float8Vector left, Float8Vector right) {
    checkCapacity(dest, left, right);
    andValidity(dest, left, right);
    int rowCount = dest.getValueCount();
    ArrowBuf destBuf = dest.getDataBuffer();
    for (int i = 0; i < rowCount; i++) {
      long index = i * (long) Float8Vector.TYPE_WIDTH;
      destBuf.setDouble(index, evaluate(left.get(i), right.get(i)));
    }
  }

  private void checkCapacity(FieldVector dest, FieldVector left, FieldVector right) {
    if (dest.getValueCount() > left.getValueCount()) {
      throw new IllegalArgumentException("The capacity of left buffer is not enough");
    }
    if (dest.getValueCount() > right.getValueCount()) {
      throw new IllegalArgumentException("The capacity of right buffer is not enough");
    }
  }

  private void andValidity(FieldVector dest, FieldVector left, FieldVector right) {
    try (And and = new And()) {
      and.evaluate(
          dest.getValidityBuffer(),
          left.getValidityBuffer(),
          right.getValidityBuffer(),
          BitVectorHelper.getValidityBufferSize(dest.getValueCount()));
    }
  }

  @Override
  public void close() {
    castFunction.close();
  }

  public abstract int evaluate(int left, int right);

  public abstract long evaluate(long left, long right);

  public abstract float evaluate(float left, float right);

  public abstract double evaluate(double left, double right);
}
