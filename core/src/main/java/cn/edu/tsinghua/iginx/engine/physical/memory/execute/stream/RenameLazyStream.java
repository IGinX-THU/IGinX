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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import java.util.Map;

public class RenameLazyStream extends UnaryLazyStream {

  private final Rename rename;

  private Header header;

  public RenameLazyStream(Rename rename, RowStream stream) {
    super(stream);
    this.rename = rename;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      Header header = stream.getHeader();
      Map<String, String> aliasMap = rename.getAliasMap();

      this.header = header.renamedHeader(aliasMap, rename.getIgnorePatterns());
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
    if (header.hasKey()) {
      return new Row(header, row.getKey(), row.getValues());
    } else {
      return new Row(header, row.getValues());
    }
  }
}
