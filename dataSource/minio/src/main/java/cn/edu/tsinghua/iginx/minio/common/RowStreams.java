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
package cn.edu.tsinghua.iginx.minio.common;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeFieldRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.List;
import javax.annotation.Nullable;

public final class RowStreams {

  private RowStreams() {}

  public static RowStream empty() {
    return new EmptyRowStream();
  }

  public static RowStream empty(Header header) {
    return new EmptyRowStream(header);
  }

  /** merge streams by key, columns with the same name and type are merged */
  public static RowStream union(List<RowStream> streams) throws PhysicalException {
    if (streams.isEmpty()) {
      return empty();
    } else if (streams.size() == 1) {
      return streams.get(0);
    }
    return new MergeFieldRowStreamWrapper(streams);
  }

  public static RowStream filtered(RowStream stream, @Nullable Filter filter) {
    if (Filters.isTrue(filter)) {
      return stream;
    }
    return new FilterRowStreamWrapper(stream, filter);
  }

  /** close all streams, the first exception is thrown with others suppressed */
  public static void close(Iterable<? extends RowStream> streams) throws PhysicalException {
    PhysicalException exception = null;
    for (RowStream stream : streams) {
      try {
        stream.close();
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
