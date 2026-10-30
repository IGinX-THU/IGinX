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
package cn.edu.tsinghua.iginx.minio.kv;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/** Merges sorted columns into rows ordered by key, and filters rows by the select filter. */
final class ColumnsRowStream implements RowStream {

  private final Header header;
  private final List<Segment> columns;
  private final int[] cursors;
  private final Filter filter;
  private Row nextRow;

  ColumnsRowStream(Header header, List<Segment> columns, @Nullable Filter filter) {
    this.header = header;
    this.columns = columns;
    this.cursors = new int[columns.size()];
    this.filter = filter;
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNext() throws PhysicalException {
    while (nextRow == null) {
      Row row = merge();
      if (row == null) {
        return false;
      }
      if (filter == null || FilterUtils.validate(filter, row)) {
        nextRow = row;
      }
    }
    return true;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }

  @Nullable
  private Row merge() {
    boolean found = false;
    long key = Long.MAX_VALUE;
    for (int i = 0; i < columns.size(); i++) {
      Segment column = columns.get(i);
      if (cursors[i] < column.size()) {
        long candidate = column.getKeys()[cursors[i]];
        if (!found || candidate < key) {
          key = candidate;
          found = true;
        }
      }
    }
    if (!found) {
      return null;
    }
    Object[] values = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Segment column = columns.get(i);
      if (cursors[i] < column.size() && column.getKeys()[cursors[i]] == key) {
        values[i] = column.getValues()[cursors[i]];
        cursors[i]++;
      }
    }
    return new Row(header, key, values);
  }
}
