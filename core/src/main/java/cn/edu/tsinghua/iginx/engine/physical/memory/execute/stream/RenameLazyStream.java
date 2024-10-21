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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.HashSet;

public class RenameLazyStream extends UnaryLazyStream {

  private final Rename rename;

  private Header header;

  private int colIndex;

  private final HashSet<Long> keySet;

  public RenameLazyStream(Rename rename, RowStream stream) {
    super(stream);
    this.rename = rename;
    this.keySet = new HashSet<>();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      Header header = stream.getHeader();
      Pair<Header, Integer> pair =
          header.renamedHeader(rename.getAliasList(), rename.getIgnorePatterns());
      this.header = pair.k;
      this.colIndex = pair.v;
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return stream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    Row row = stream.next();
    if (colIndex == -1) {
      return new Row(header, row.getKey(), row.getValues());
    } else {
      Row newRow = RowUtils.transformColumnToKey(header, row, colIndex);
      if (keySet.contains(newRow.getKey())) {
        throw new PhysicalTaskExecuteFailureException("duplicated key found: " + newRow.getKey());
      }
      keySet.add(newRow.getKey());
      return newRow;
    }
  }
}
