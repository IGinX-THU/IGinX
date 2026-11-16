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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import java.util.Objects;
import org.apache.arrow.vector.*;

public abstract class ColumnBuilder {

  protected int index = 0;

  protected FieldVector wrapped;

  protected ColumnBuilder(FieldVector wrapped) {
    this.wrapped = Objects.requireNonNull(wrapped);
  }

  protected abstract void setNotNull(Object value);

  public ColumnBuilder append(Object value) {
    if (value == null) {
      wrapped.setNull(index);
    } else {
      setNotNull(value);
    }
    index++;
    return this;
  }

  public static boolean support(FieldVector wrapped) {
    switch (wrapped.getMinorType()) {
      case INT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
      case BIT:
      case VARBINARY:
        return true;
      default:
        return false;
    }
  }

  public static ColumnBuilder create(FieldVector wrapped) {
    switch (wrapped.getMinorType()) {
      case INT:
        return new IntegerBuilder((IntVector) wrapped);
      case BIGINT:
        return new LongBuilder((BigIntVector) wrapped);
      case FLOAT4:
        return new FloatBuilder((Float4Vector) wrapped);
      case FLOAT8:
        return new DoubleBuilder((Float8Vector) wrapped);
      case BIT:
        return new BooleanBuilder((BitVector) wrapped);
      case VARBINARY:
        return new BinaryBuilder((VarBinaryVector) wrapped);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + wrapped.getMinorType());
    }
  }

  public static class BooleanBuilder extends ColumnBuilder {

    protected final BitVector wrapped;

    protected BooleanBuilder(BitVector wrapped) {
      super(wrapped);
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    protected void setNotNull(Object value) {
      wrapped.set(index, (Boolean) value ? 1 : 0);
    }
  }

  public static class IntegerBuilder extends ColumnBuilder {

    protected final IntVector wrapped;

    protected IntegerBuilder(IntVector wrapped) {
      super(wrapped);
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    protected void setNotNull(Object value) {
      wrapped.set(index, (Integer) value);
    }
  }

  public static class LongBuilder extends ColumnBuilder {

    protected final BigIntVector wrapped;

    protected LongBuilder(BigIntVector wrapped) {
      super(wrapped);
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    protected void setNotNull(Object value) {
      wrapped.set(index, (Long) value);
    }
  }

  public static class FloatBuilder extends ColumnBuilder {

    protected final Float4Vector wrapped;

    protected FloatBuilder(Float4Vector wrapped) {
      super(wrapped);
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    protected void setNotNull(Object value) {
      wrapped.set(index, (Float) value);
    }
  }

  public static class DoubleBuilder extends ColumnBuilder {

    protected final Float8Vector wrapped;

    protected DoubleBuilder(Float8Vector wrapped) {
      super(wrapped);
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    protected void setNotNull(Object value) {
      wrapped.set(index, (Double) value);
    }
  }

  public static class BinaryBuilder extends ColumnBuilder {

    protected final VarBinaryVector wrapped;

    protected BinaryBuilder(VarBinaryVector wrapped) {
      super(wrapped);
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    protected void setNotNull(Object value) {
      wrapped.setSafe(index, (byte[]) value);
    }
  }
}
