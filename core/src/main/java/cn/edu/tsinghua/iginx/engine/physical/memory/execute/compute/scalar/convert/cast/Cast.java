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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

public final class Cast extends AbstractScalarCast<FieldVector> {

  private final AbstractScalarCast<?> delegate;

  public Cast(Types.MinorType resultType) throws ComputeException {
    super(resultType);
    this.delegate = getDelegate(resultType);
  }

  @Override
  public FieldVector evaluate(BufferAllocator allocator, FieldVector input)
      throws ComputeException {
    return delegate.evaluate(allocator, input);
  }

  private AbstractScalarCast<?> getDelegate(Types.MinorType resultType) throws ComputeException {
    switch (resultType) {
      case BIT:
        return new CastAsBit();
      case BIGINT:
        return new CastAsBigInt();
      case FLOAT8:
        return new CastAsFloat8();
      default:
        throw new ComputeException("Cast to " + resultType + " is not supported");
    }
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) && delegate.equals(((Cast) obj).delegate);
  }
}
