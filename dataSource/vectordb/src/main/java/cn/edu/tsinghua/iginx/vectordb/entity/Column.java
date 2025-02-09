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
package cn.edu.tsinghua.iginx.vectordb.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Column {

  private final String pathName;

  private final DataType type;

  private final Map<Long, Object> data;

  public Column(String pathName, String value) {
    this(pathName);
    data.put(0L, value);
  }

  public Column(String pathName, List<String> values) {
    this(pathName);
    for (int i = 0; i < values.size(); i++) {
      data.put((long) i, values.get(i));
    }
  }

  public Column(String pathName, Set<String> values) {
    this(pathName);
    int i = 0;
    for (String value : values) {
      data.put((long) i, value);
      i++;
    }
  }

  public Column(String pathName) {
    this.pathName = pathName;
    this.type = DataType.BINARY;
    this.data = new HashMap<>();
  }

  public Column(String pathName, DataType type, Map<Long, Object> data) {
    this.pathName = pathName;
    this.type = type;
    this.data = data;
  }

  public Column(String pathName, Map<Long, Object> data) {
    this.pathName = pathName;
    if (data != null && data.size() > 0) {
      Object firstValue = data.values().iterator().next();
      if (firstValue instanceof String) {
        type = DataType.BINARY;
      } else if (firstValue instanceof Long) {
        type = DataType.LONG;
      } else if (firstValue instanceof Double) {
        type = DataType.DOUBLE;
      } else if (firstValue instanceof Float) {
        type = DataType.FLOAT;
      } else if (firstValue instanceof Integer) {
        type = DataType.INTEGER;
      } else if (firstValue instanceof Boolean) {
        type = DataType.BOOLEAN;
      } else {
        type = DataType.BINARY;
      }
    } else {
      type = DataType.BINARY;
    }
    this.data = data;
  }

  public String getPathName() {
    return pathName;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, Object> getData() {
    return data;
  }
}
