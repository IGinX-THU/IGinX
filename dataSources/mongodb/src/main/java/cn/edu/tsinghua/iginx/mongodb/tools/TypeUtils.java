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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.bson.*;

public class TypeUtils {

  public static Object toObject(BsonValue value) {
    switch (value.getBsonType()) {
      case DOUBLE:
        return value.asDouble().getValue();
      case STRING:
        return value.asString().getValue().getBytes();
      case BINARY:
        return bytesToFloat(value.asBinary().getData());
      case BOOLEAN:
        return value.asBoolean().getValue();
      case INT32:
        return value.asInt32().getValue();
      case INT64:
        return value.asInt64().getValue();
      default:
        throw new IllegalStateException("unexpected type: " + value.getBsonType());
    }
  }

  public static BsonValue toBsonValue(DataType type, Object value) {
    switch (type) {
      case BOOLEAN:
        return new BsonBoolean((Boolean) value);
      case INTEGER:
        return new BsonInt32((Integer) value);
      case LONG:
        return new BsonInt64((Long) value);
      case FLOAT:
        return new BsonBinary(floatToBytes((Float) value));
      case DOUBLE:
        return new BsonDouble((Double) value);
      case BINARY:
        return new BsonString(new String((byte[]) value));
      default:
        throw new IllegalArgumentException("unknown data type: " + type);
    }
  }

  private static byte[] floatToBytes(float num) {
    int bits = 0;
    if (num != 0.0) {
      bits ^= Float.floatToIntBits(num) ^ 0x80000000;
    }

    byte byte0 = (byte) (bits >>> 24);
    byte byte1 = (byte) ((bits >> 16) & 0xff);
    byte byte2 = (byte) ((bits >> 8) & 0xff);
    byte byte3 = (byte) (bits & 0xff);

    if (byte3 != 0) {
      return new byte[] {byte0, byte1, byte2, byte3};
    } else if (byte2 != 0) {
      return new byte[] {byte0, byte1, byte2};
    } else if (byte1 != 0) {
      return new byte[] {byte0, byte1};
    } else if (byte0 != 0) {
      return new byte[] {byte0};
    } else {
      return new byte[] {};
    }
  }

  private static float bytesToFloat(byte[] bytes) {
    int bits = 0;
    switch (bytes.length) {
      default:
      case 4:
        bits |= (bytes[3] & 0xff);
      case 3:
        bits |= ((bytes[2] & 0xff) << 8);
      case 2:
        bits |= ((bytes[1] & 0xff) << 16);
      case 1:
        bits |= ((bytes[0] & 0xff) << 24);
        break;
      case 0:
        return 0f;
    }

    return Float.intBitsToFloat(bits ^ 0x80000000);
  }
}
