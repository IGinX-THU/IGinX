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
package cn.edu.tsinghua.iginx.engine.shared.function.system.utils;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class ValueUtils {

  private static final Set<DataType> numericTypeSet =
      new HashSet<>(
          Arrays.asList(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE));

  public static boolean isNumericType(Value value) {
    return numericTypeSet.contains(value.getDataType());
  }

  public static boolean isNumericType(DataType dataType) {
    return numericTypeSet.contains(dataType);
  }

  public static Value transformToDouble(Value value) {
    DataType dataType = value.getDataType();
    double dVal;
    switch (dataType) {
      case INTEGER:
        dVal = value.getIntV().doubleValue();
        break;
      case LONG:
        dVal = value.getLongV().doubleValue();
        break;
      case FLOAT:
        dVal = value.getFloatV().doubleValue();
        break;
      case DOUBLE:
        dVal = value.getDoubleV();
        break;
      case BOOLEAN:
        dVal = value.getBoolV() ? 1.0D : 0.0D;
        break;
      case BINARY:
        dVal = Double.parseDouble(value.getBinaryVAsString());
        break;
      default:
        throw new IllegalArgumentException("Unexpected dataType: " + dataType);
    }
    return new Value(DataType.DOUBLE, dVal);
  }

  public static int compare(Value v1, Value v2) throws PhysicalException {
    DataType dataType1 = v1.getDataType();
    DataType dataType2 = v2.getDataType();
    if (dataType1 != dataType2) {
      if (numericTypeSet.contains(dataType1) && numericTypeSet.contains(dataType2)) {
        v1 = transformToDouble(v1);
        v2 = transformToDouble(v2);
        dataType1 = DataType.DOUBLE;
      } else {
        throw new InvalidOperatorParameterException(
            dataType1.toString() + " and " + dataType2.toString() + " can't be compared");
      }
    }
    switch (dataType1) {
      case INTEGER:
        return Integer.compare(v1.getIntV(), v2.getIntV());
      case LONG:
        return Long.compare(v1.getLongV(), v2.getLongV());
      case BOOLEAN:
        return Boolean.compare(v1.getBoolV(), v2.getBoolV());
      case FLOAT:
        return Float.compare(v1.getFloatV(), v2.getFloatV());
      case DOUBLE:
        return Double.compare(v1.getDoubleV(), v2.getDoubleV());
      case BINARY:
        return v1.getBinaryVAsString().compareTo(v2.getBinaryVAsString());
    }
    return 0;
  }

  public static boolean regexCompare(Value value, Value regex) {
    if (!value.getDataType().equals(DataType.BINARY)
        || !regex.getDataType().equals(DataType.BINARY)) {
      // regex can only be compared between strings.
      return false;
    }

    String valueStr = value.getBinaryVAsString();
    String regexStr = regex.getBinaryVAsString();

    return Pattern.matches(regexStr, valueStr);
  }

  public static int compare(Object o1, Object o2, DataType dataType) {
    switch (dataType) {
      case INTEGER:
        return Integer.compare((Integer) o1, (Integer) o2);
      case LONG:
        return Long.compare((Long) o1, (Long) o2);
      case BOOLEAN:
        return Boolean.compare((Boolean) o1, (Boolean) o2);
      case FLOAT:
        return Float.compare((Float) o1, (Float) o2);
      case DOUBLE:
        return Double.compare((Double) o1, (Double) o2);
      case BINARY:
        return (new String((byte[]) o1)).compareTo(new String((byte[]) o2));
    }
    return 0;
  }

  public static int compare(Object o1, Object o2, DataType dataType1, DataType dataType2)
      throws PhysicalException {
    if (dataType1 != dataType2) {
      if (numericTypeSet.contains(dataType1) && numericTypeSet.contains(dataType2)) {
        Value v1 = ValueUtils.transformToDouble(new Value(dataType1, o1));
        Value v2 = ValueUtils.transformToDouble(new Value(dataType2, o2));
        return compare(v1, v2);
      } else {
        throw new InvalidOperatorParameterException(
            dataType1.toString() + " and " + dataType2.toString() + " can't be compared");
      }
    } else {
      return compare(o1, o2, dataType1);
    }
  }

  public static String toString(Object value, DataType dataType) {
    switch (dataType) {
      case INTEGER:
      case LONG:
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
        return value.toString();
      case BINARY:
        return new String((byte[]) value);
    }
    return "";
  }

  public static int getHash(Value value, boolean needTypeCast) {
    if (needTypeCast) {
      value = ValueUtils.transformToDouble(value);
    }
    if (value.getDataType() == DataType.BINARY) {
      return Arrays.hashCode(value.getBinaryV());
    } else {
      return value.getValue().hashCode();
    }
  }

  public static Comparator<Row> firstLastRowComparator() {
    return (o1, o2) -> {
      if (o1.getKey() < o2.getKey()) {
        return -1;
      } else if (o1.getKey() > o2.getKey()) {
        return 1;
      }
      String s1 = new String((byte[]) o1.getValue(0));
      String s2 = new String((byte[]) o2.getValue(0));
      return s1.compareTo(s2);
    };
  }

  public static Object[] moveForwardNotNull(Object[] values) {
    Object[] newValues = new Object[values.length];
    int index = 0;
    for (Object value : values) {
      if (value != null) {
        newValues[index] = value;
        index++;
      }
    }
    return newValues;
  }
}
