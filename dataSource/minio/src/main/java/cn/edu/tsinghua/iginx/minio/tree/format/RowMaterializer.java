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
package cn.edu.tsinghua.iginx.minio.tree.format;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import shaded.iginx.org.apache.parquet.io.api.Binary;
import shaded.iginx.org.apache.parquet.io.api.Converter;
import shaded.iginx.org.apache.parquet.io.api.GroupConverter;
import shaded.iginx.org.apache.parquet.io.api.PrimitiveConverter;
import shaded.iginx.org.apache.parquet.io.api.RecordMaterializer;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

/** Assembles a record of flat primitive columns into values in the order of schema fields. */
final class RowMaterializer extends RecordMaterializer<Object[]> {

  private final Root root;

  RowMaterializer(MessageType schema) {
    this.root = new Root(schema);
  }

  @Override
  public Object[] getCurrentRecord() {
    return root.values;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

  private static final class Root extends GroupConverter {

    private final Converter[] converters;
    private final ByteArrayOutputStream[] repeated;
    private Object[] values;

    Root(MessageType schema) {
      int n = schema.getFieldCount();
      this.converters = new Converter[n];
      this.repeated = new ByteArrayOutputStream[n];
      for (int i = 0; i < n; i++) {
        PrimitiveType type = schema.getType(i).asPrimitiveType();
        if (type.isRepetition(Type.Repetition.REPEATED)) {
          repeated[i] = new ByteArrayOutputStream();
          converters[i] = new RepeatedConverter(repeated[i]);
        } else {
          converters[i] = new ValueConverter(i);
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      values = new Object[converters.length];
      for (ByteArrayOutputStream buffer : repeated) {
        if (buffer != null) {
          buffer.reset();
        }
      }
    }

    @Override
    public void end() {
      for (int i = 0; i < repeated.length; i++) {
        if (repeated[i] != null && repeated[i].size() > 0) {
          values[i] = repeated[i].toByteArray();
        }
      }
    }

    private final class ValueConverter extends PrimitiveConverter {
      private final int index;

      ValueConverter(int index) {
        this.index = index;
      }

      @Override
      public void addBoolean(boolean value) {
        values[index] = value;
      }

      @Override
      public void addInt(int value) {
        values[index] = value;
      }

      @Override
      public void addLong(long value) {
        values[index] = value;
      }

      @Override
      public void addFloat(float value) {
        values[index] = value;
      }

      @Override
      public void addDouble(double value) {
        values[index] = value;
      }

      @Override
      public void addBinary(Binary value) {
        values[index] = value.getBytes();
      }
    }
  }

  private static final class RepeatedConverter extends PrimitiveConverter {
    private final ByteArrayOutputStream buffer;

    RepeatedConverter(ByteArrayOutputStream buffer) {
      this.buffer = buffer;
    }

    @Override
    public void addBoolean(boolean value) {
      buffer.write(value ? 0xFF : 0);
    }

    @Override
    public void addInt(int value) {
      buffer.write(Ints.toByteArray(value), 0, Integer.BYTES);
    }

    @Override
    public void addLong(long value) {
      buffer.write(Longs.toByteArray(value), 0, Long.BYTES);
    }

    @Override
    public void addFloat(float value) {
      addInt(Float.floatToIntBits(value));
    }

    @Override
    public void addDouble(double value) {
      addLong(Double.doubleToLongBits(value));
    }

    @Override
    public void addBinary(Binary value) {
      byte[] bytes = value.getBytes();
      buffer.write(bytes, 0, bytes.length);
    }
  }
}
