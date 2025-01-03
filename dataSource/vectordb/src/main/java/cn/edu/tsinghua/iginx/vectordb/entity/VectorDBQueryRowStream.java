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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.tools.DataTransformer;
import cn.edu.tsinghua.iginx.vectordb.tools.TagKVUtils;
import java.util.*;

public class VectorDBQueryRowStream implements RowStream {

  private List<Column> columns;

  private List<Long> times;

  private final Header header;

  private final Filter filter;

  private Row nextRow = null;

  private int cur = 0;

  public VectorDBQueryRowStream(List<Column> columns, Filter filter) {
    this.columns = columns;

    Set<Long> timeSet = new TreeSet<>();
    List<Field> fields = new ArrayList<>();
    for (Column column : columns) {
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(column.getPathName());
      fields.add(new Field(pair.getK(), column.getType(), pair.getV()));
      timeSet.addAll(column.getData().keySet());
    }
    this.times = new ArrayList<>(timeSet);
    this.header = new Header(Field.KEY, fields);
    this.filter = filter;
  }

  public VectorDBQueryRowStream(List<Column> columns) {
    this(columns, null);
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    columns.clear();
    times.clear();
    columns = null;
    times = null;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null && cur < times.size()) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new PhysicalException("no more data");
    }

    Row currRow = nextRow;
    nextRow = calculateNext();
    return currRow;
  }

  private Row calculateNext() throws PhysicalException {
    while (cur < times.size()) {
      long timestamp = times.get(cur);
      cur++;

      Object[] values = new Object[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        DataType type = columns.get(i).getType();
        if (columns.get(i).getData().get(timestamp) != null) {
          values[i] = DataTransformer.toIginxType(columns.get(i).getData().get(timestamp));
        }
      }

      Row row = new Row(header, timestamp, values);
      if (filter == null || FilterUtils.validate(filter, row)) return row;
    }

    return null;
  }
}
