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
package cn.edu.tsinghua.iginx.minio.tree.format;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.minio.common.Patterns;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import javax.annotation.Nullable;

/**
 * Any object as a single BINARY column named by its prefix, the n-th row is the n-th page of {@code
 * pageSize} bytes.
 */
final class RawChunkFormat implements ObjectFormat {

  static final String NAME = "RawChunk";

  static final String PAGE_SIZE = "pageSize";

  private static final long DEFAULT_PAGE_SIZE = 4096;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<String> getExtensions() {
    return Collections.emptyList();
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) {
    long pageSize = Configs.getBytes(config, PAGE_SIZE, DEFAULT_PAGE_SIZE);
    if (pageSize <= 0 || pageSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(PAGE_SIZE + " should be in (0, 2GB): " + pageSize);
    }
    return new RawChunkReader(String.valueOf(prefix), path, (int) pageSize);
  }

  @Override
  public String toString() {
    return NAME;
  }

  private static final class RawChunkReader implements Reader {

    private final String field;
    private final Path path;
    private final int pageSize;

    RawChunkReader(String field, Path path, int pageSize) {
      this.field = field;
      this.path = path;
      this.pageSize = pageSize;
    }

    @Override
    public Map<String, DataType> find(Collection<String> patterns) {
      if (!Patterns.match(patterns, field)) {
        return Collections.emptyMap();
      }
      return Collections.singletonMap(field, DataType.BINARY);
    }

    @Override
    public RowStream read(List<String> fields, RangeSet<Long> keys) throws IOException {
      if (!fields.equals(Collections.singletonList(field))) {
        throw new IllegalArgumentException("unknown fields: " + fields);
      }
      Header header =
          new Header(Field.KEY, Collections.singletonList(new Field(field, DataType.BINARY)));
      return new RawChunkRowStream(header, Files.newByteChannel(path), pageSize, keys);
    }
  }

  private static final class RawChunkRowStream implements RowStream {

    private final Header header;
    private final SeekableByteChannel channel;
    private final int pageSize;
    private final KeyCursor cursor;
    private Row next;

    RawChunkRowStream(Header header, SeekableByteChannel channel, int pageSize, RangeSet<Long> keys)
        throws IOException {
      this.header = header;
      this.channel = channel;
      this.pageSize = pageSize;
      long pages = (channel.size() + pageSize - 1) / pageSize;
      this.cursor = new KeyCursor(keys, pages);
      this.next = fetch();
    }

    @Nullable
    private Row fetch() throws IOException {
      if (!cursor.advance()) {
        return null;
      }
      long key = cursor.key();
      channel.position(key * pageSize);
      ByteBuffer buffer = ByteBuffer.allocate(pageSize);
      while (buffer.hasRemaining() && channel.read(buffer) > 0) {}
      if (buffer.position() == 0) {
        return null;
      }
      byte[] data = Arrays.copyOf(buffer.array(), buffer.position());
      return new Row(header, key, new Object[] {data});
    }

    @Override
    public Header getHeader() {
      return header;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Row next() throws PhysicalException {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Row row = next;
      try {
        next = fetch();
      } catch (IOException e) {
        throw new PhysicalException("failed to read " + header.getField(0).getName(), e);
      }
      return row;
    }

    @Override
    public void close() throws PhysicalException {
      try {
        channel.close();
      } catch (IOException e) {
        throw new PhysicalException(e);
      }
    }
  }
}
