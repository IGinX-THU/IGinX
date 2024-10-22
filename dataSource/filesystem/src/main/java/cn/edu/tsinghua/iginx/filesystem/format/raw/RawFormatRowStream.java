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
package cn.edu.tsinghua.iginx.filesystem.format.raw;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemRowStream;
import cn.edu.tsinghua.iginx.filesystem.common.Ranges;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Queue;
import javax.annotation.Nullable;

public class RawFormatRowStream extends FileSystemRowStream {

  private final Header header;
  private final FileChannel channel;
  private final long pageSize;
  private final Queue<Range<Long>> keyRanges;
  private long nextFetchKey = 0;
  private boolean eof = false;
  private Row nextRow;

  public RawFormatRowStream(Header header, Path path, long pageSize, RangeSet<Long> keyRanges)
      throws IOException {
    this.header = header;
    this.channel = FileChannel.open(path, StandardOpenOption.READ);
    this.pageSize = pageSize;

    this.keyRanges = new ArrayDeque<>(keyRanges.asRanges().size());
    for (Range<Long> range : keyRanges.asRanges()) {
      Range<Long> closedRange = Ranges.toClosedLongRange(range);
      if (!closedRange.isEmpty()) {
        this.keyRanges.add(closedRange);
      }
    }

    this.nextRow = fetchNext();
  }

  @Override
  public Header getHeader() throws FileSystemException {
    return header;
  }

  @Override
  public void close() throws FileSystemException {
    try {
      channel.close();
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public boolean hasNext() throws FileSystemException {
    return nextRow != null;
  }

  @Override
  public Row next() throws FileSystemException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Row row = nextRow;
    try {
      nextRow = fetchNext();
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
    return row;
  }

  public boolean needFetch() {
    if (eof) {
      return false;
    }
    while (!keyRanges.isEmpty()) {
      Range<Long> range = keyRanges.peek();
      if (nextFetchKey < range.lowerEndpoint()) {
        nextFetchKey = range.lowerEndpoint();
      }
      if (nextFetchKey <= range.upperEndpoint()) {
        return true;
      }
      keyRanges.poll();
    }
    return false;
  }

  @Nullable
  public Row fetchNext() throws IOException {
    if (!needFetch()) {
      return null;
    }
    long currentKey = nextFetchKey++;
    byte[] data = new byte[Math.toIntExact(pageSize)];
    ByteBuffer buffer = ByteBuffer.wrap(data);
    channel.position(currentKey * pageSize);
    channel.read(buffer);
    if (buffer.remaining() > 0) {
      eof = true;
      if (buffer.position() == 0) {
        return null;
      }
      data = Arrays.copyOf(data, buffer.position());
    }
    Object[] values = new Object[] {data};
    return new Row(header, currentKey, values);
  }
}
