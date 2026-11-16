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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.arithmetic;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.CallContracts;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;

public abstract class UnaryArithmeticScalarFunction extends UnaryScalarFunction<FieldVector> {

  public UnaryArithmeticScalarFunction(String name) {
    super(name);
  }

  @Override
  public FieldVector evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector in)
      throws ComputeException {
    if (selection == null) {
      return evaluate(allocator, in);
    }
    try (FieldVector selected = PhysicalFunctions.take(allocator, selection, in)) {
      return evaluate(allocator, selected);
    }
  }

  public FieldVector evaluate(BufferAllocator allocator, FieldVector in) throws ComputeException {
    CallContracts.ensureType(this, Schemas.of(in), Schemas::isNumeric);

    FieldVector dest = ValueVectors.create(allocator, in.getMinorType(), in.getValueCount());
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
        break;
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

  protected abstract int evaluate(int value);

  protected abstract long evaluate(long value);

  protected abstract float evaluate(float value);

  protected abstract double evaluate(double value);
}
