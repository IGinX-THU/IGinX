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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.convert.CastNumericAsFloat8;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

import java.util.function.IntConsumer;

public abstract class BinaryArithmeticFunction extends BinaryFunction<FieldVector> {

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
    if (left.getMinorType() == right.getMinorType()) {
      return evaluateSameType(context, left, right);
    } else {
      try (CastNumericAsFloat8 castFunction = new CastNumericAsFloat8();
           FieldVector leftCast = castFunction.evaluate(context, left);
           FieldVector rightCast = castFunction.evaluate(context, right)) {
        return evaluateSameType(context, leftCast, rightCast);
      }
    }
  }

  private FieldVector evaluateSameType(ExecutorContext context, FieldVector left, FieldVector right) {
    int rowCount = Math.min(left.getValueCount(), right.getValueCount());
    if (left instanceof NullVector || right instanceof NullVector) {
      return ConstantVectors.ofNull(context.getAllocator(), left.getValueCount());
    }

    FieldVector dest = ValueVectors.create(context.getAllocator(), left.getMinorType(), rowCount);
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

  private void evaluate(IntVector dest, IntVector left, IntVector right) {
    genericEvaluate(dest, left, right, i -> dest.set(i, evaluate(left.get(i), right.get(i))));
  }

  private void evaluate(BigIntVector dest, BigIntVector left, BigIntVector right) {
    genericEvaluate(dest, left, right, i -> dest.set(i, evaluate(left.get(i), right.get(i))));
  }

  private void evaluate(Float4Vector dest, Float4Vector left, Float4Vector right) {
    genericEvaluate(dest, left, right, i -> dest.set(i, evaluate(left.get(i), right.get(i))));
  }

  private void evaluate(Float8Vector dest, Float8Vector left, Float8Vector right) {
    genericEvaluate(dest, left, right, i -> dest.set(i, evaluate(left.get(i), right.get(i))));
  }

  private void genericEvaluate(FieldVector dest, FieldVector left, FieldVector right, IntConsumer consumer) {
    if (dest.getValueCount() > left.getValueCount()) {
      throw new IllegalArgumentException("The capacity of left buffer is not enough");
    }
    if (dest.getValueCount() > right.getValueCount()) {
      throw new IllegalArgumentException("The capacity of right buffer is not enough");
    }
    int rowCount = dest.getValueCount();
    for (int i = 0; i < rowCount; i++) {
      if (!left.isNull(i) && !right.isNull(i)) {
        consumer.accept(i);
      }
    }
  }

  @Override
  public void close() {
  }

  protected abstract int evaluate(int left, int right);

  protected abstract long evaluate(long left, long right);

  protected abstract float evaluate(float left, float right);

  protected abstract double evaluate(double left, double right);
}
