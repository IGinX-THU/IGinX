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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import java.util.Objects;
import org.apache.arrow.vector.*;

public abstract class ColumnBuilder {

  protected int index = 0;

  protected abstract ColumnBuilder append(Object value);

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
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public BooleanBuilder append(Object value) {
      if (Objects.isNull(value)) {
        wrapped.setNull(index);
      } else {
        wrapped.setSafe(index, (Boolean) value ? 1 : 0);
      }
      index++;
      return this;
    }
  }

  public static class IntegerBuilder extends ColumnBuilder {

    protected final IntVector wrapped;

    protected IntegerBuilder(IntVector wrapped) {
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public IntegerBuilder append(Object value) {
      if (Objects.isNull(value)) {
        wrapped.setNull(index);
      } else {
        wrapped.setSafe(index, (Integer) value);
      }
      index++;
      return this;
    }
  }

  public static class LongBuilder extends ColumnBuilder {

    protected final BigIntVector wrapped;

    protected LongBuilder(BigIntVector wrapped) {
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public LongBuilder append(Object value) {
      if (Objects.isNull(value)) {
        wrapped.setNull(index);
      } else {
        wrapped.setSafe(index, (Long) value);
      }
      index++;
      return this;
    }
  }

  public static class FloatBuilder extends ColumnBuilder {

    protected final Float4Vector wrapped;

    protected FloatBuilder(Float4Vector wrapped) {
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public FloatBuilder append(Object value) {
      if (Objects.isNull(value)) {
        wrapped.setNull(index);
      } else {
        wrapped.setSafe(index, (Float) value);
      }
      index++;
      return this;
    }
  }

  public static class DoubleBuilder extends ColumnBuilder {

    protected final Float8Vector wrapped;

    protected DoubleBuilder(Float8Vector wrapped) {
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public DoubleBuilder append(Object value) {
      if (Objects.isNull(value)) {
        wrapped.setNull(index);
      } else {
        wrapped.setSafe(index, (Double) value);
      }
      index++;
      return this;
    }
  }

  public static class BinaryBuilder extends ColumnBuilder {

    protected final VarBinaryVector wrapped;

    protected BinaryBuilder(VarBinaryVector wrapped) {
      this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public BinaryBuilder append(Object value) {
      if (Objects.isNull(value)) {
        wrapped.setNull(index);
      } else {
        wrapped.setSafe(index, (byte[]) value);
      }
      index++;
      return this;
    }
  }
}
