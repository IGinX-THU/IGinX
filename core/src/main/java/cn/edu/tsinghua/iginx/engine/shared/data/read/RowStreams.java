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
import cn.edu.tsinghua.iginx.engine.physical.task.utils.PhysicalCloseable;
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

  private static class ExtraClosableRowStream implements RowStream {

    private final RowStream delegate;
    private final PhysicalCloseable closeable;

    public ExtraClosableRowStream(RowStream delegate, PhysicalCloseable closeable) {
      this.delegate = Objects.requireNonNull(delegate);
      this.closeable = Objects.requireNonNull(closeable);
    }

    @Override
    public Header getHeader() throws PhysicalException {
      return delegate.getHeader();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      return delegate.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
      return delegate.next();
    }

    @Override
    public void close() throws PhysicalException {
      try {
        delegate.close();
      } finally {
        closeable.close();
      }
    }
  }

  public static RowStream closeWith(RowStream resultStream, PhysicalCloseable closeable) {
    return new ExtraClosableRowStream(resultStream, closeable);
  }
}
