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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class Point {

  private final String path;

  private final DataType dataType;

  private final long timestamp;

  private final Object value;

  public Point(String path, DataType dataType, long timestamp, Object value) {
    this.path = path;
    this.dataType = dataType;
    this.timestamp = timestamp;
    this.value = value;
  }

  public String getPath() {
    return path;
  }

  public DataType getDataType() {
    return dataType;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object getValue() {
    return value;
  }
}
