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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.Cast;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

public final class Ratio extends BinaryArithmeticScalarFunction {

  public Ratio() {
    super("ratio");
  }

  @Override
  protected int evaluate(int left, int right) {
    throw new UnsupportedOperationException("Unreachable");
  }

  @Override
  protected long evaluate(long left, long right) {
    throw new UnsupportedOperationException("Unreachable");
  }

  @Override
  protected float evaluate(float left, float right) {
    return left / right;
  }

  @Override
  protected double evaluate(double left, double right) {
    return left / right;
  }

  @Override
  public FieldVector evaluate(BufferAllocator allocator, FieldVector left, FieldVector right)
      throws ComputeException {
    if (left.getMinorType() == right.getMinorType()) {
      if (left.getMinorType() == Types.MinorType.INT
          || left.getMinorType() == Types.MinorType.BIGINT) {
        Cast castFunction = new Cast(Types.MinorType.FLOAT8);
        try (FieldVector leftCast = castFunction.evaluate(allocator, left);
            FieldVector rightCast = castFunction.evaluate(allocator, right)) {
          return super.evaluate(allocator, leftCast, rightCast);
        }
      }
    }
    return super.evaluate(allocator, left, right);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Ratio;
  }
}
