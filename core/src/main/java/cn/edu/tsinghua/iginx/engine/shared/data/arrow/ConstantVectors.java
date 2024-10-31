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
package cn.edu.tsinghua.iginx.engine.shared.data.arrow;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import javax.annotation.Nullable;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class ConstantVectors {

  public static FieldVector of(
      @WillNotClose BufferAllocator allocator, @Nullable Object value, int valueCount) {
    if (value instanceof Value) {
      return of(allocator, (Value) value, valueCount);
    } else {
      return of(allocator, new Value(value), valueCount);
    }
  }

  public static FieldVector of(
      @WillNotClose BufferAllocator allocator, @Nullable Value value, int valueCount) {
    if (value == null) {
      return ofNull(allocator, valueCount);
    } else if (value.isNull()) {
      return new NullVector(
          Field.nullable(null, Schemas.toArrowType(value.getDataType())), valueCount);
    }
    switch (value.getDataType()) {
      case BOOLEAN:
        return of(allocator, (boolean) value.getBoolV(), valueCount);
      case INTEGER:
        return of(allocator, (int) value.getIntV(), valueCount);
      case LONG:
        return of(allocator, (long) value.getLongV(), valueCount);
      case FLOAT:
        return of(allocator, (float) value.getFloatV(), valueCount);
      case DOUBLE:
        return of(allocator, (double) value.getDoubleV(), valueCount);
      case BINARY:
        return of(allocator, value.getBinaryV(), valueCount);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static FieldVector ofNull(@WillNotClose BufferAllocator allocator, int valueCount) {
    return new NullVector((String) null, valueCount);
  }

  public static FieldVector of(
      @WillNotClose BufferAllocator allocator, boolean value, int valueCount) {
    BitVector vector =
        new BitVector(Schemas.nullableField(String.valueOf(value), Types.MinorType.BIT), allocator);
    vector.allocateNew(valueCount);
    setValueCountWithValidity(vector, valueCount);
    if (value) {
      vector.setRangeToOne(0, valueCount);
    }
    return vector;
  }

  public static FieldVector of(@WillNotClose BufferAllocator allocator, int value, int valueCount) {
    IntVector vector =
        new IntVector(Schemas.nullableField(String.valueOf(value), Types.MinorType.INT), allocator);
    vector.allocateNew(valueCount);
    setValueCountWithValidity(vector, valueCount);
    if (value != 0) {
      setAllWithoutValidity(vector, value);
    }
    return vector;
  }

  public static FieldVector of(
      @WillNotClose BufferAllocator allocator, long value, int valueCount) {
    BigIntVector vector =
        new BigIntVector(
            Schemas.nullableField(String.valueOf(value), Types.MinorType.BIGINT), allocator);
    vector.allocateNew(valueCount);
    setValueCountWithValidity(vector, valueCount);
    if (value != 0) {
      setAllWithoutValidity(vector, value);
    }
    return vector;
  }

  public static FieldVector of(
      @WillNotClose BufferAllocator allocator, float value, int valueCount) {
    Float4Vector vector =
        new Float4Vector(
            Schemas.nullableField(String.valueOf(value), Types.MinorType.FLOAT4), allocator);
    vector.allocateNew(valueCount);
    setValueCountWithValidity(vector, valueCount);
    if (value != 0) {
      setAllWithoutValidity(vector, Float.floatToRawIntBits(value));
    }
    return vector;
  }

  public static FieldVector of(
      @WillNotClose BufferAllocator allocator, double value, int valueCount) {
    Float8Vector vector =
        new Float8Vector(
            Schemas.nullableField(String.valueOf(value), Types.MinorType.FLOAT8), allocator);
    vector.allocateNew(valueCount);
    setValueCountWithValidity(vector, valueCount);
    if (value != 0) {
      setAllWithoutValidity(vector, Double.doubleToRawLongBits(value));
    }
    return vector;
  }

  public static FieldVector of(BufferAllocator allocator, byte[] value, int valueCount) {
    VarBinaryVector vector =
        new VarBinaryVector(
            Schemas.nullableField(new String(value), Types.MinorType.VARBINARY), allocator);
    vector.allocateNew(valueCount * (long) value.length, valueCount);
    setValueCountWithValidity(vector, valueCount);
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();
    ArrowBuf valueBuffer = vector.getDataBuffer();
    for (int i = 0; i < valueCount; i++) {
      int valueOffset = i * value.length;
      offsetBuffer.setInt(i * (long) VarBinaryVector.OFFSET_WIDTH, valueOffset);
      valueBuffer.setBytes(valueOffset, value);
    }
    offsetBuffer.setInt(
        valueCount * (long) VarBinaryVector.OFFSET_WIDTH, valueCount * value.length);
    return vector;
  }

  public static void setValueCountWithValidity(@WillNotClose ValueVector vector, int valueCount) {
    vector.setValueCount(valueCount);
    long byteCount = BitVectorHelper.getValidityBufferSize(valueCount);
    vector.getValidityBuffer().setOne(0, byteCount);
  }

  private static void setAllWithoutValidity(@WillNotClose BaseFixedWidthVector vector, int value) {
    ArrowBuf dataBuffer = vector.getDataBuffer();
    for (long i = 0; i < vector.getValueCount(); i++) {
      dataBuffer.setInt(i * 4, value);
    }
  }

  private static void setAllWithoutValidity(@WillNotClose BaseFixedWidthVector vector, long value) {
    ArrowBuf dataBuffer = vector.getDataBuffer();
    for (long i = 0; i < vector.getValueCount(); i++) {
      dataBuffer.setLong(i * 8, value);
    }
  }
}
