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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.common;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.List;
import javax.annotation.Nullable;

public class RowStreams {

  private RowStreams() {}

  private static final EmptyRowStream EMPTY = new EmptyRowStream();

  public static RowStream empty() {
    return EMPTY;
  }

  public static RowStream empty(Header header) {
    return new EmptyRowStream(header);
  }

  public static RowStream union(List<RowStream> rowStreams) throws PhysicalException {
    if (rowStreams.isEmpty()) {
      return empty();
    } else if (rowStreams.size() == 1) {
      return rowStreams.get(0);
    } else {
      return new MergeFieldRowStreamWrapper(rowStreams);
    }
  }

  public static RowStream filtered(RowStream rowStream, @Nullable Filter filter) {
    if (Filters.isTrue(filter)) {
      return rowStream;
    }
    return new FilterRowStreamWrapper(rowStream, filter);
  }
}
