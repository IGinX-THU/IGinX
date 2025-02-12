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
package cn.edu.tsinghua.iginx.redis.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Column {

  private final String pathName;

  private final DataType type;

  private final Map<Long, String> data;

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

  public Column(String pathName, DataType type, Map<Long, String> data) {
    this.pathName = pathName;
    this.type = type;
    this.data = data;
  }

  public String getPathName() {
    return pathName;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, String> getData() {
    return data;
  }
}
