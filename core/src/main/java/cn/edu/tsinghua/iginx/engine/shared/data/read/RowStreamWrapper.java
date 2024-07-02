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

public class RowStreamWrapper implements RowStream {

  private final RowStream rowStream;

  private Row nextRow; // 如果不为空，表示 row stream 的下一行已经取出来，并缓存在 wrapper 里

  public RowStreamWrapper(RowStream rowStream) {
    this.rowStream = rowStream;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return rowStream.getHeader();
  }

  @Override
  public void close() throws PhysicalException {
    rowStream.close();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return nextRow != null || rowStream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    Row row = null;
    if (nextRow != null) { // 本地已经缓存了下一行
      row = nextRow;
      nextRow = null;
    } else {
      row = rowStream.next();
    }
    return row;
  }

  public long nextTimestamp() throws PhysicalException {
    if (nextRow == null) { // 本地已经缓存了下一行
      nextRow = rowStream.next();
    }
    return nextRow.getKey();
  }
}
