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
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
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
 * Any object as a single BINARY column named by its prefix, without parsing its content. Same as
 * the LegacyFilesystem structure of filesystem, the key of a row is the byte offset where its chunk
 * begins, and the value is at most {@code chunkSize} bytes from that offset. Chunks of a range of
 * keys begin at the lower bound of the range, so the first key of a full scan is 0.
 */
final class ByteStreamFormat implements ObjectFormat {

  static final String NAME = "ByteStream";

  private final int chunkSize;

  ByteStreamFormat(int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunk size should be positive: " + chunkSize);
    }
    this.chunkSize = chunkSize;
  }

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
    return new ByteStreamReader(String.valueOf(prefix), path, chunkSize);
  }

  @Override
  public String toString() {
    return NAME + "{chunkSize=" + chunkSize + '}';
  }

  private static final class ByteStreamReader implements Reader {

    private final String field;
    private final Path path;
    private final int chunkSize;

    ByteStreamReader(String field, Path path, int chunkSize) {
      this.field = field;
      this.path = path;
      this.chunkSize = chunkSize;
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
      return new ByteStreamRowStream(header, Files.newByteChannel(path), chunkSize, keys);
    }
  }

  private static final class ByteStreamRowStream implements RowStream {

    private final Header header;
    private final SeekableByteChannel channel;
    private final int chunkSize;

    /** closed ranges of offsets in ascending order, offsets beyond the object are excluded */
    private final Deque<long[]> ranges = new ArrayDeque<>();

    private long offset;
    private Row next;

    ByteStreamRowStream(
        Header header, SeekableByteChannel channel, int chunkSize, RangeSet<Long> keys)
        throws IOException {
      this.header = header;
      this.channel = channel;
      this.chunkSize = chunkSize;
      long size = channel.size();
      for (Range<Long> range : keys.subRangeSet(Range.closedOpen(0L, size)).asRanges()) {
        long lower = range.lowerEndpoint();
        if (range.lowerBoundType() == BoundType.OPEN) {
          lower++;
        }
        long upper = range.upperEndpoint();
        if (range.upperBoundType() == BoundType.OPEN) {
          upper--;
        }
        if (lower <= upper) {
          ranges.add(new long[] {lower, upper});
        }
      }
      if (!ranges.isEmpty()) {
        offset = ranges.peekFirst()[0];
      }
      this.next = fetch();
    }

    @Nullable
    private Row fetch() throws IOException {
      while (!ranges.isEmpty() && offset > ranges.peekFirst()[1]) {
        ranges.pollFirst();
        if (!ranges.isEmpty()) {
          offset = Math.max(offset, ranges.peekFirst()[0]);
        }
      }
      if (ranges.isEmpty()) {
        return null;
      }
      long key = offset;
      channel.position(key);
      ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
      while (buffer.hasRemaining() && channel.read(buffer) > 0) {}
      if (buffer.position() == 0) {
        return null;
      }
      offset = key + chunkSize;
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
