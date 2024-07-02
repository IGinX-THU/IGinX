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
package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;

public class SourceTable implements Iterable<SourceTable.Column> {

  private final DataView dataView;

  public SourceTable(DataView dataView) {
    this.dataView = dataView;
  }

  public static class Column {

    private final Field field;

    private final Map<Long, Object> data;

    private Column(String name, Map<String, String> tags, DataType type, Map<Long, Object> data) {
      this.field = new Field(name, type, tags);
      this.data = data;
    }

    public Field getField() {
      return field;
    }

    public Map<Long, Object> getData() {
      return data;
    }
  }

  @Override
  public Iterator<Column> iterator() {
    if (this.dataView.isColumnData()) {
      return createColumnDataIterator();
    } else {
      return createRowDataIterator();
    }
  }

  private Iterator<Column> createColumnDataIterator() {
    List<Column> columns = new ArrayList<>();
    for (int pathIdx = 0; pathIdx < this.dataView.getPathNum(); pathIdx++) {
      String name = this.dataView.getPaths().get(pathIdx);
      Map<String, String> tags = this.dataView.getTags(pathIdx);
      if (tags == null) tags = Collections.emptyMap();
      DataType type = this.dataView.getDataType(pathIdx);
      BitmapView bitmap = this.dataView.getBitmapView(pathIdx);
      Map<Long, Object> data = new HashMap<>();
      for (int keyIdx = 0, cnt = 0; keyIdx < dataView.getKeySize(); keyIdx++) {
        if (bitmap.get(keyIdx)) {
          long key = this.dataView.getKey(keyIdx);
          Object value = this.dataView.getValue(pathIdx, cnt++);
          data.put(key, value);
        }
      }
      columns.add(new Column(name, tags, type, data));
    }
    return columns.iterator();
  }

  private Iterator<Column> createRowDataIterator() {
    Map<Integer, Map<Long, Object>> dataList = new HashMap<>();
    for (int keyIdx = 0; keyIdx < this.dataView.getKeySize(); keyIdx++) {
      long key = this.dataView.getKey(keyIdx);
      BitmapView bitmap = this.dataView.getBitmapView(keyIdx);
      for (int pathIdx = 0, cnt = 0; pathIdx < this.dataView.getPathNum(); pathIdx++) {
        if (bitmap.get(pathIdx)) {
          Object value = this.dataView.getValue(keyIdx, cnt++);
          dataList.computeIfAbsent(pathIdx, k -> new HashMap<>()).put(key, value);
        }
      }
    }
    List<Column> columns = new ArrayList<>();
    for (int pathIdx = 0; pathIdx < this.dataView.getPathNum(); pathIdx++) {
      String name = this.dataView.getPaths().get(pathIdx);
      Map<String, String> tags = this.dataView.getTags(pathIdx);
      if (tags == null) tags = Collections.emptyMap();
      DataType type = this.dataView.getDataType(pathIdx);
      columns.add(new Column(name, tags, type, dataList.get((pathIdx))));
    }
    return columns.iterator();
  }
}
