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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;

public class RowStreams {

  private RowStreams() {}

  private static class EmptyRowStream implements RowStream {

    private final Header header;

    public EmptyRowStream() {
      this(new Header(Collections.emptyList()));
    }

    public EmptyRowStream(Header header) {
      this.header = Objects.requireNonNull(header);
    }

    @Override
    public Header getHeader() throws PhysicalException {
      return header;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Row next() {
      throw new NoSuchElementException();
    }

    @Override
    public void close() {}
  }

  private static final EmptyRowStream EMPTY = new EmptyRowStream();

  public static RowStream empty() {
    return EMPTY;
  }
}
