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
package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import java.util.List;
import java.util.Map;

public class RawData {

  private final List<String> paths;

  private final List<Map<String, String>> tagsList;

  private final List<Long> keys;

  private final Object[] valuesList;

  private final List<DataType> dataTypeList;

  private final List<Bitmap> bitmaps;

  private final RawDataType type;

  public RawData(
      List<String> paths,
      List<Map<String, String>> tagsList,
      List<Long> keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Bitmap> bitmaps,
      RawDataType type) {
    this.paths = paths;
    this.tagsList = tagsList;
    this.keys = keys;
    this.valuesList = valuesList;
    this.dataTypeList = dataTypeList;
    this.bitmaps = bitmaps;
    this.type = type;
  }

  public List<String> getPaths() {
    return paths;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  public List<Long> getKeys() {
    return keys;
  }

  public Object[] getValuesList() {
    return valuesList;
  }

  public List<DataType> getDataTypeList() {
    return dataTypeList;
  }

  public List<Bitmap> getBitmaps() {
    return bitmaps;
  }

  public RawDataType getType() {
    return type;
  }

  public boolean isRowData() {
    return type == RawDataType.Row || type == RawDataType.NonAlignedRow;
  }

  public boolean isColumnData() {
    return type == RawDataType.Column || type == RawDataType.NonAlignedColumn;
  }
}
