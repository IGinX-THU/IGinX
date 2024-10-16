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
    if (left.getValueCount() != right.getValueCount()) {
      throw new IllegalArgumentException(
          "Invalid number of rows for argument "
              + left.getField()
              + " and "
              + right.getField()
              + " for function "
              + getName());
    }
    ArrowBuf leftBuf = left.getDataBuffer();
    ArrowBuf rightBuf = right.getDataBuffer();
    int rowCount = left.getValueCount();
    switch (left.getMinorType()) {
      case INT: {
        IntVector dest =
            ValueVectors.likeIntersection(
                context.getAllocator(), (IntVector) left, (IntVector) right);
        ArrowBuf destBuf = dest.getDataBuffer();
        for (long i = 0; i < rowCount; i++) {
          long index = i * IntVector.TYPE_WIDTH;
          destBuf.setInt(index, evaluate(leftBuf.getInt(index), rightBuf.getInt(index)));
        }
        return dest;
      }
      case BIGINT: {
        BigIntVector dest =
            ValueVectors.likeIntersection(
                context.getAllocator(), (BigIntVector) left, (BigIntVector) right);
        ArrowBuf destBuf = dest.getDataBuffer();
        for (long i = 0; i < rowCount; i++) {
          long index = i * BigIntVector.TYPE_WIDTH;
          destBuf.setLong(index, evaluate(leftBuf.getLong(index), rightBuf.getLong(index)));
        }
        return dest;
      }
      case FLOAT4: {
        Float4Vector dest =
            ValueVectors.likeIntersection(
                context.getAllocator(), (Float4Vector) left, (Float4Vector) right);
        ArrowBuf destBuf = dest.getDataBuffer();
        for (long i = 0; i < rowCount; i++) {
          long index = i * Float4Vector.TYPE_WIDTH;
          destBuf.setFloat(index, evaluate(leftBuf.getFloat(index), rightBuf.getFloat(index)));
        }
        return dest;
      }
      case FLOAT8: {
        Float8Vector dest =
            ValueVectors.likeIntersection(
                context.getAllocator(), (Float8Vector) left, (Float8Vector) right);
        ArrowBuf destBuf = dest.getDataBuffer();
        for (long i = 0; i < rowCount; i++) {
          long index = i * Float8Vector.TYPE_WIDTH;
          destBuf.setDouble(index, evaluate(leftBuf.getDouble(index), rightBuf.getDouble(index)));
        }
        return dest;
      }
      default:
        throw new IllegalStateException("Unsupported type: " + left.getMinorType());
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
