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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;

public class CastAsBigInt extends AbstractScalarCast<BigIntVector> {

  public CastAsBigInt() {
    super(Types.MinorType.BIGINT);
  }

  @Override
  protected void evaluate(BigIntVector dest, IntVector input) throws ComputeException {
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!input.isNull(i)) {
        dest.set(i, input.get(i));
      } else {
        dest.setNull(i);
      }
    }
  }

  @Override
  protected void evaluate(BigIntVector dest, Float4Vector input) throws ComputeException {
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!input.isNull(i)) {
        dest.set(i, (long) input.get(i));
      } else {
        dest.setNull(i);
      }
    }
  }

  @Override
  protected void evaluate(BigIntVector dest, Float8Vector input) throws ComputeException {
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!input.isNull(i)) {
        dest.set(i, (long) input.get(i));
      } else {
        dest.setNull(i);
      }
    }
  }
}
