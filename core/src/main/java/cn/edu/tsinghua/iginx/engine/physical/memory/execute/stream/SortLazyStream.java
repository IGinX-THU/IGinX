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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import java.util.ArrayList;
import java.util.List;

public class SortLazyStream extends UnaryLazyStream {

  private final Sort sort;

  private final boolean asc;

  private final List<Row> rows;

  private boolean hasSorted = false;

  private int cur = 0;

  public SortLazyStream(Sort sort, RowStream stream) {
    super(stream);
    this.sort = sort;
    this.asc = sort.getSortType() == Sort.SortType.ASC;
    this.rows = new ArrayList<>();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasSorted) {
      while (stream.hasNext()) {
        rows.add(stream.next());
      }
      RowUtils.sortRows(rows, asc, sort.getSortByCols());
      hasSorted = true;
    }
    return cur < rows.size();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return rows.get(cur++);
  }
}
