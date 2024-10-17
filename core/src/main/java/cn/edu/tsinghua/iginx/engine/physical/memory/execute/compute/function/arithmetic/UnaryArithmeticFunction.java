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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.UnaryFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;
import java.util.function.IntConsumer;

public abstract class UnaryArithmeticFunction extends UnaryFunction<FieldVector> {

  public UnaryArithmeticFunction(String name) {
    super(name);
  }

  @Override
  protected boolean allowType(Types.MinorType type) {
    return Schemas.isNumeric(type);
  }

  @Override
  public FieldVector evaluate(ExecutorContext context, @WillNotClose FieldVector in) throws ComputeException {
    if (in instanceof NullVector) {
      return ConstantVectors.ofNull(context.getAllocator(), in.getValueCount());
    }

    FieldVector dest = ValueVectors.create(context.getAllocator(), in.getMinorType(), in.getValueCount());
    switch (in.getMinorType()) {
      case INT:
        evaluate((IntVector) dest, (IntVector) in);
        break;
      case BIGINT:
        evaluate((BigIntVector) dest, (BigIntVector) in);
        break;
      case FLOAT4:
        evaluate((Float4Vector) dest, (Float4Vector) in);
        break;
      case FLOAT8:
        evaluate((Float8Vector) dest, (Float8Vector) in);
      default:
        throw new IllegalStateException("Unsupported type: " + in.getMinorType());
    }
    return dest;
  }

  private void evaluate(IntVector out, IntVector in) {
    genericEvaluate(out, in, i -> out.set(i, evaluate(in.get(i))));
  }

  private void evaluate(BigIntVector out, BigIntVector in) {
    genericEvaluate(out, in, i -> out.set(i, evaluate(in.get(i))));
  }

  private void evaluate(Float4Vector out, Float4Vector in) {
    genericEvaluate(out, in, i -> out.set(i, evaluate(in.get(i))));
  }

  private void evaluate(Float8Vector out, Float8Vector in) {
    genericEvaluate(out, in, i -> out.set(i, evaluate(in.get(i))));
  }

  private void genericEvaluate(FieldVector out, FieldVector in, IntConsumer consumer) {
    if (out.getValueCount() > in.getValueCount()) {
      throw new IllegalArgumentException("The capacity of in buffer is not enough");
    }
    int rowCount = out.getValueCount();
    for (int i = 0; i < rowCount; i++) {
      if (!in.isNull(i)) {
        consumer.accept(i);
      }
    }
  }

  @Override
  public void close() {
  }

  protected abstract int evaluate(int value);

  protected abstract long evaluate(long value);

  protected abstract float evaluate(float value);

  protected abstract double evaluate(double value);
}
