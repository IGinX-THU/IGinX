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
package cn.edu.tsinghua.iginx.minio.kv;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

/**
 * Binary content of one segment object: sorted keys and values of one column.
 *
 * <pre>
 * magic "IGKV" | version:byte | type:byte | count:int | keys:long[count] | values[count]
 * </pre>
 *
 * BINARY values are stored as length:int followed by bytes.
 */
public final class Segment {

  private static final int MAGIC = 0x49474B56; // "IGKV"
  private static final byte VERSION = 1;

  private final DataType type;
  private final long[] keys;
  private final Object[] values;

  public Segment(DataType type, long[] keys, Object[] values) {
    if (keys.length != values.length) {
      throw new IllegalArgumentException("keys and values have different length");
    }
    this.type = Objects.requireNonNull(type);
    this.keys = keys;
    this.values = values;
  }

  public static Segment of(DataType type, NavigableMap<Long, Object> data) {
    long[] keys = new long[data.size()];
    Object[] values = new Object[data.size()];
    int i = 0;
    for (Map.Entry<Long, Object> entry : data.entrySet()) {
      keys[i] = entry.getKey();
      values[i] = entry.getValue();
      i++;
    }
    return new Segment(type, keys, values);
  }

  public DataType getType() {
    return type;
  }

  public long[] getKeys() {
    return keys;
  }

  public Object[] getValues() {
    return values;
  }

  public int size() {
    return keys.length;
  }

  public boolean isEmpty() {
    return keys.length == 0;
  }

  public long minKey() {
    return keys[0];
  }

  public long maxKey() {
    return keys[keys.length - 1];
  }

  public byte[] encode() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(16 + keys.length * 16);
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(MAGIC);
      out.writeByte(VERSION);
      out.writeByte(type.getValue());
      out.writeInt(keys.length);
      for (long key : keys) {
        out.writeLong(key);
      }
      for (Object value : values) {
        writeValue(out, value);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return bytes.toByteArray();
  }

  private void writeValue(DataOutputStream out, Object value) throws IOException {
    switch (type) {
      case BOOLEAN:
        out.writeBoolean((Boolean) value);
        break;
      case INTEGER:
        out.writeInt((Integer) value);
        break;
      case LONG:
        out.writeLong((Long) value);
        break;
      case FLOAT:
        out.writeFloat((Float) value);
        break;
      case DOUBLE:
        out.writeDouble((Double) value);
        break;
      case BINARY:
        byte[] binary = (byte[]) value;
        out.writeInt(binary.length);
        out.write(binary);
        break;
      default:
        throw new IllegalArgumentException("unsupported data type: " + type);
    }
  }

  public static Segment decode(byte[] content) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
      if (in.readInt() != MAGIC) {
        throw new IOException("not a segment object");
      }
      byte version = in.readByte();
      if (version != VERSION) {
        throw new IOException("unsupported segment version: " + version);
      }
      DataType type = DataType.findByValue(in.readByte());
      if (type == null) {
        throw new IOException("unknown data type in segment");
      }
      int count = in.readInt();
      long[] keys = new long[count];
      for (int i = 0; i < count; i++) {
        keys[i] = in.readLong();
      }
      Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        values[i] = readValue(in, type);
      }
      return new Segment(type, keys, values);
    }
  }

  private static Object readValue(DataInputStream in, DataType type) throws IOException {
    switch (type) {
      case BOOLEAN:
        return in.readBoolean();
      case INTEGER:
        return in.readInt();
      case LONG:
        return in.readLong();
      case FLOAT:
        return in.readFloat();
      case DOUBLE:
        return in.readDouble();
      case BINARY:
        byte[] binary = new byte[in.readInt()];
        in.readFully(binary);
        return binary;
      default:
        throw new IOException("unsupported data type: " + type);
    }
  }
}
