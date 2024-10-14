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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.AbstractFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.cast.CastNumericAsFloat8;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Objects;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

public abstract class ArithmeticFunction extends AbstractFunction {

  private final String name;
  private final Types.MinorType returnType;
  private final Types.MinorType leftType;
  private final Types.MinorType rightType;
  private final CastNumericAsFloat8 leftCast;
  private final CastNumericAsFloat8 rightCast;

  protected ArithmeticFunction(String name, Types.MinorType leftType, Types.MinorType rightType) {
    super(Arity.BINARY);
    this.name = Objects.requireNonNull(name);
    this.returnType = determineResultType(leftType, rightType);
    this.leftType = leftType;
    this.rightType = rightType;
    this.leftCast = new CastNumericAsFloat8(leftType);
    this.rightCast = new CastNumericAsFloat8(rightType);
  }

  protected abstract int evaluate(int left, int right);

  protected abstract long evaluate(long left, long right);

  protected abstract float evaluate(float left, float right);

  protected abstract double evaluate(double left, double right);

  @Override
  protected Types.MinorType getReturnTypeImpl(ExecutorContext context, Types.MinorType... args) {
    return returnType;
  }

  @Override
  protected ValueVector invokeImpl(ExecutorContext context, int rowCount, ValueVector... args) {
    if (leftType == rightType) {
      return evaluate(context, args[0], args[1]);
    } else {
      try (Float8Vector left = leftCast.evaluate(context, args[0]);
          Float8Vector right = rightCast.evaluate(context, args[1])) {
        return evaluate(context, left, right);
      }
    }
  }

  private ValueVector evaluate(ExecutorContext context, ValueVector left, ValueVector right) {
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
      case INT:
        {
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
      case BIGINT:
        {
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
      case FLOAT4:
        {
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
      case FLOAT8:
        {
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
    leftCast.close();
    rightCast.close();
  }

  @Override
  protected boolean allowType(int index, Types.MinorType type) {
    if (index == 0) {
      return type == leftType;
    } else if (index == 1) {
      return type == rightType;
    } else {
      return false;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  public static Types.MinorType determineResultType(
      Types.MinorType leftType, Types.MinorType rightType) {
    if (!Schemas.isNumeric(leftType) || !Schemas.isNumeric(rightType)) {
      throw new IllegalStateException("Unsupported type: " + leftType + " and " + rightType);
    }
    if (leftType == rightType) {
      return leftType;
    } else {
      return Types.MinorType.FLOAT8;
    }
  }
}
