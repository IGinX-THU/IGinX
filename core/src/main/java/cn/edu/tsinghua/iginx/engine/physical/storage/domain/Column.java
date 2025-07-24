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
package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.*;
import java.util.stream.Collectors;

public final class Column {

  private String path;

  private final Map<String, String> tags;

  private final DataType dataType;

  private String physicalPath = null;

  private boolean isDummy = false;

  public Column(String path, DataType dataType) {
    this(path, dataType, null);
  }

  public Column(String path, DataType dataType, Map<String, String> tags) {
    this.path = path;
    this.dataType = dataType;
    this.tags = tags;
  }

  public Column(String path, DataType dataType, Map<String, String> tags, boolean isDummy) {
    this(path, dataType, tags);
    this.isDummy = isDummy;
  }

  public static RowStream toRowStream(Collection<Column> timeseries) {
    List<Row> rows =
        timeseries.stream()
            .map(e -> toRow(Header.SHOW_COLUMNS_HEADER, e))
            .collect(Collectors.toList());
    return new Table(Header.SHOW_COLUMNS_HEADER, rows);
  }

  public static Row toRow(Header header, Column column) {
    return new Row(
        header,
        new Object[] {
          TagKVUtils.toFullName(column.path, column.tags).getBytes(),
          column.dataType.toString().getBytes()
        });
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getPhysicalPath() {
    if (physicalPath == null) {
      physicalPath = TagKVUtils.toPhysicalPath(path, tags);
    }
    return physicalPath;
  }

  public DataType getDataType() {
    return dataType;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public boolean isDummy() {
    return isDummy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column that = (Column) o;
    return Objects.equals(path, that.path)
        && dataType == that.dataType
        && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, dataType, tags);
  }
}
