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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.NoSuchElementException;

public class FilterRowStreamWrapper implements RowStream {

  private final RowStream stream;

  private final Filter filter;

  private Row nextRow;

  public FilterRowStreamWrapper(RowStream stream, Filter filter) {
    this.stream = stream;
    this.filter = filter;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public void close() throws PhysicalException {
    stream.close();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow != null) {
      return true;
    }
    nextRow = loadNextRow();
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new RowFetchException(new NoSuchElementException());
    }

    Row row = nextRow;
    nextRow = null;
    return row;
  }

  private Row loadNextRow() throws PhysicalException {
    while (stream.hasNext()) {
      Row row = stream.next();
      if (FilterUtils.validate(filter, row)) {
        return row;
      }
    }
    return null;
  }
}
