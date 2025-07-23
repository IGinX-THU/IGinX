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

  public static void close(Iterable<? extends RowStream> rowStreams) throws PhysicalException {
    PhysicalException exception = null;
    for (RowStream rowStream : rowStreams) {
      try {
        rowStream.close();
      } catch (PhysicalException e) {
        if (exception == null) {
          exception = e;
        } else if (e != exception) {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }
}
