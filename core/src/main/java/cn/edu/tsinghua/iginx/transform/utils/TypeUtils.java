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

package cn.edu.tsinghua.iginx.transform.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class TypeUtils {

  public static DataType arrowTypeToDataType(ArrowType type) {
    switch (type.toString()) {
      case "Int(64, true)":
        return DataType.LONG;
      case "Int(32, true)":
        return DataType.INTEGER;
      case "Bool":
        return DataType.BOOLEAN;
      case "FloatingPoint(SINGLE)":
        return DataType.FLOAT;
      case "FloatingPoint(DOUBLE)":
        return DataType.DOUBLE;
      case "Utf8":
        return DataType.BINARY;
      default:
        throw new IllegalArgumentException(
            String.format("Can not convert %s to iginx DataType", type.toString()));
    }
  }

  public static FieldVector getFieldVectorByType(
      String name, DataType dataType, RootAllocator allocator) {
    switch (dataType) {
      case LONG:
        return new BigIntVector(name, allocator);
      case INTEGER:
        return new IntVector(name, allocator);
      case BOOLEAN:
        return new BitVector(name, allocator);
      case FLOAT:
        return new Float4Vector(name, allocator);
      case DOUBLE:
        return new Float8Vector(name, allocator);
      case BINARY:
        return new VarCharVector(name, allocator);
      default:
        return null;
    }
  }

  public static void setValue(FieldVector vector, int index, DataType dataType, Object object) {
    switch (dataType) {
      case LONG:
        ((BigIntVector) vector).setSafe(index, (long) object);
        break;
      case INTEGER:
        ((IntVector) vector).setSafe(index, (int) object);
        break;
      case BOOLEAN:
        ((BitVector) vector).setSafe(index, (boolean) object ? 1 : 0);
        break;
      case FLOAT:
        ((Float4Vector) vector).setSafe(index, (float) object);
        break;
      case DOUBLE:
        ((Float8Vector) vector).setSafe(index, (double) object);
        break;
      case BINARY:
        ((VarCharVector) vector).setSafe(index, (byte[]) object);
        break;
    }
  }

  public static ValueVector getValueVectorByDataType(
      DataType dataType, String name, RootAllocator allocator) {
    switch (dataType) {
      case LONG:
        return new BigIntVector(name, allocator);
      case INTEGER:
        return new IntVector(name, allocator);
      case BOOLEAN:
        return new BitVector(name, allocator);
      case FLOAT:
        return new Float4Vector(name, allocator);
      case DOUBLE:
        return new Float8Vector(name, allocator);
      case BINARY:
        return new VarCharVector(name, allocator);
      default:
        return null;
    }
  }
}
