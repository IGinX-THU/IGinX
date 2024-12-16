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
package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.BsonType;
import org.bson.BsonValue;

class ResultColumn {

  private final DataType type;

  private final Map<Long, Object> data;

  public ResultColumn(DataType type, Map<Long, Object> data) {
    this.type = type;
    this.data = data;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, Object> getData() {
    return data;
  }

  public static class Builder {

    private final Collection<SimpleImmutableEntry<Long, BsonValue>> values = new ArrayList<>();
    private DataType type;

    public void add(long key, BsonValue value) {
      values.add(new SimpleImmutableEntry<>(key, value));
    }

    ResultColumn build() {
      try {
        return buildByConvert();
      } catch (Exception e) {
        return buildAsBinary();
      }
    }

    private ResultColumn buildByConvertToNotBinary(DataType type) {
      Map<Long, Object> data = new HashMap<>();
      for (SimpleImmutableEntry<Long, BsonValue> bsonValue : values) {
        Object value = TypeUtils.convertToNotBinaryWithIgnore(bsonValue.getValue(), type);
        data.put(bsonValue.getKey(), value);
      }
      return new ResultColumn(type, data);
    }

    private ResultColumn buildByConvert() {
      DataType type = analysisType();
      if (type == DataType.BINARY) {
        return buildAsBinary();
      }
      Map<Long, Object> data = new HashMap<>();
      for (SimpleImmutableEntry<Long, BsonValue> bsonValue : values) {
        Object convertedValue = TypeUtils.convertTo(bsonValue.getValue(), type);
        if (convertedValue != null) {
          data.put(bsonValue.getKey(), convertedValue);
        }
      }
      return new ResultColumn(type, data);
    }

    private DataType analysisType() {
      Map<BsonType, Long> typeNum =
          values.stream()
              .map(Map.Entry::getValue)
              .collect(Collectors.groupingBy(BsonValue::getBsonType, Collectors.counting()));

      typeNum.remove(BsonType.STRING);
      typeNum.remove(BsonType.DOCUMENT);
      typeNum.remove(BsonType.ARRAY);
      typeNum.remove(BsonType.BINARY);
      typeNum.remove(BsonType.UNDEFINED);
      typeNum.remove(BsonType.NULL);
      typeNum.remove(BsonType.JAVASCRIPT);
      typeNum.remove(BsonType.JAVASCRIPT_WITH_SCOPE);
      typeNum.remove(BsonType.REGULAR_EXPRESSION);
      typeNum.remove(BsonType.SYMBOL);

      DataType expectedType = type;
      if (expectedType == null) {
        BsonType majorType =
            typeNum.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseGet(() -> new SimpleImmutableEntry<>(BsonType.BINARY, 0L))
                .getKey();
        expectedType = TypeUtils.convert(majorType);
      }

      DataType resultType = expectedType;
      switch (expectedType) {
        case INTEGER:
          if (typeNum.containsKey(BsonType.INT64)) {
            resultType = DataType.LONG;
          }
        case LONG:
          if (typeNum.containsKey(BsonType.DOUBLE)) {
            resultType = DataType.DOUBLE;
          }
      }
      return resultType;
    }

    private ResultColumn buildAsBinary() {
      DataType type = DataType.BINARY;
      Map<Long, Object> data = new HashMap<>();
      for (SimpleImmutableEntry<Long, BsonValue> entry : values) {
        Long key = entry.getKey();
        BsonValue bsonValue = entry.getValue();
        byte[] value = TypeUtils.convertToBinary(bsonValue);
        data.put(key, value);
      }
      return new ResultColumn(type, data);
    }

    public void setType(DataType type) {
      this.type = type;
    }
  }
}
