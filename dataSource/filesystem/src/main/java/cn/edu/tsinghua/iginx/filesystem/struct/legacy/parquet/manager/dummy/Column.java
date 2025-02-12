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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class Column {

  private String pathName;

  private final String physicalPath;

  private final DataType type;

  private final Map<Long, Object> data = new HashMap<>();

  public Column(String pathName, String physicalPath, DataType type) {
    this.pathName = pathName;
    this.physicalPath = physicalPath;
    this.type = type;
  }

  public void putData(long time, Object value) {
    data.put(time, value);
  }

  public void putBatchData(Map<Long, Object> batchData) {
    data.putAll(batchData);
  }

  public void removeData(long time) {
    data.remove(time);
  }

  public String getPathName() {
    return pathName;
  }

  public String getPhysicalPath() {
    return physicalPath;
  }

  public DataType getType() {
    return type;
  }

  public Map<Long, Object> getData() {
    return data;
  }

  public void setPathName(String pathName) {
    this.pathName = pathName;
  }
}
