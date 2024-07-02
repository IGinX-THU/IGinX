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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.Collections;
import java.util.List;

public class Table implements RowStream {

  public static final Table EMPTY_TABLE = new Table(Header.EMPTY_HEADER, Collections.emptyList());

  private final Header header;

  private final List<Row> rows;

  private int index;

  private RequestContext context;

  public Table(Header header, List<Row> rows) {
    this.header = header;
    this.rows = rows;
    this.index = 0;
  }

  @Override
  public RequestContext getContext() {
    return context;
  }

  @Override
  public void setContext(RequestContext context) {
    this.context = context;
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public boolean hasNext() {
    return index < rows.size();
  }

  public boolean isEmpty() {
    return rows == null || rows.isEmpty();
  }

  @Override
  public Row next() {
    Row row = rows.get(index);
    index++;
    return row;
  }

  public List<Row> getRows() {
    return rows;
  }

  public Row getRow(int index) {
    return rows.get(index);
  }

  public int getRowSize() {
    return rows.size();
  }

  @Override
  public void close() {}

  public void reset() {
    this.index = 0;
  }
}
