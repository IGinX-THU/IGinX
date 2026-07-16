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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Arrays;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;

public class CastAsBigInt extends AbstractScalarCast<BigIntVector> {

  public CastAsBigInt() {
    super(Types.MinorType.BIGINT);
  }

  public CastAsBigInt(boolean isNullable) {
    super(Types.MinorType.BIGINT, isNullable);
  }

  @Override
  protected void set(BigIntVector dest, int index, boolean value) throws ComputeException {
    set(dest, index, value ? 1 : 0);
  }

  @Override
  protected void set(BigIntVector dest, int index, int value) throws ComputeException {
    set(dest, index, (long) value);
  }

  @Override
  protected void set(BigIntVector dest, int index, long value) throws ComputeException {
    dest.set(index, value);
  }

  @Override
  protected void set(BigIntVector dest, int index, float value) throws ComputeException {
    set(dest, index, (double) value);
  }

  @Override
  protected void set(BigIntVector dest, int index, double value) throws ComputeException {
    if (value > Long.MAX_VALUE) {
      throw new ComputeException("Overflow: value " + value + " is too large for long.");
    }
    if (value < Long.MIN_VALUE) {
      throw new ComputeException("Overflow: value " + value + " is too small for long.");
    }
    set(dest, index, Math.round(value));
  }

  @Override
  protected void set(BigIntVector dest, int index, byte[] value) throws ComputeException {
    try {
      set(dest, index, Long.parseLong(new String(value)));
    } catch (NumberFormatException e) {
      throw new ComputeException("Invalid value for long: " + Arrays.toString(value));
    }
  }
}
