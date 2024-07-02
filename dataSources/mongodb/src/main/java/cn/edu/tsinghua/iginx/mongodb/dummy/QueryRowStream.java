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
package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;

class QueryRowStream implements RowStream {

  private final Header header;

  private final List<Map<Long, Object>> data;

  private final Filter condition;

  private final Iterator<Long> keyItr;

  public QueryRowStream(List<ResultTable> results, Filter condition) {
    List<Field> fields = new ArrayList<>();
    List<Map<Long, Object>> data = new ArrayList<>();
    SortedSet<Long> keys = new TreeSet<>();

    results.stream()
        .map(ResultTable::getColumns)
        .map(Map::entrySet)
        .flatMap(Collection::stream)
        .forEach(
            entry -> {
              String path = entry.getKey();
              ResultColumn column = entry.getValue();
              DataType type = column.getType();
              Map<Long, Object> columnData = column.getData();

              fields.add(new Field(path, type));
              data.add(columnData);
              keys.addAll(columnData.keySet());
            });

    this.header = new Header(Field.KEY, fields);
    this.data = data;
    this.condition = condition;
    this.keyItr = keys.iterator();
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public void close() {}

  private Row nextRow = null;

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      nextRow = getNextMatchRow();
    }

    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Row curr = nextRow;
    nextRow = null;
    return curr;
  }

  private Row getNextMatchRow() throws PhysicalException {
    for (Row row = getNextRow(); row != null; row = getNextRow()) {
      if (FilterUtils.validate(this.condition, row)) {
        return row;
      }
    }
    return null;
  }

  private Row getNextRow() {
    if (!keyItr.hasNext()) {
      return null;
    }

    Long key = keyItr.next();
    Object[] values = new Object[header.getFieldSize()];
    for (int idx = 0; idx < data.size(); idx++) {
      values[idx] = data.get(idx).get(key);
    }
    return new Row(header, key, values);
  }
}
