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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class ClearEmptyRowStreamWrapper implements RowStream {

  private final RowStream stream;

  private Row nextRow;

  public ClearEmptyRowStreamWrapper(RowStream stream) {
    this.stream = stream;
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
  public boolean hasNext() throws PhysicalException { // 调用 hasNext 之后，如果返回 true，那么 nextRow 必然存在
    if (nextRow != null) {
      return true;
    }
    loadNextRow();
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new PhysicalException("the row stream has used up");
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }

  private void loadNextRow() throws PhysicalException {
    if (nextRow != null) {
      return;
    }
    do {
      if (!stream.hasNext()) {
        nextRow = null;
        break;
      }
      nextRow = stream.next();
    } while (nextRow.isEmpty());
  }
}
