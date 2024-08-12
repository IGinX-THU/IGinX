package cn.edu.tsinghua.iginx.filestore.format.raw;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreRowStream;
import cn.edu.tsinghua.iginx.filestore.common.Ranges;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Queue;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;

public class RawFileRowStream extends FileStoreRowStream {

  private final Header header;
  private final FileChannel channel;
  private final long pageSize;
  private final Queue<Range<Long>> keyRanges;
  private long nextFetchKey = 0;
  private boolean eof = false;
  private Row nextRow;

  public RawFileRowStream(
      Header header,
      Path path,
      long pageSize,
      RangeSet<Long> keyRanges)
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
  public Header getHeader() throws FileStoreException {
    return header;
  }

  @Override
  public void close() throws FileStoreException {
    try {
      channel.close();
    } catch (IOException e) {
      throw new FileStoreException(e);
    }
  }

  @Override
  public boolean hasNext() throws FileStoreException {
    return nextRow != null;
  }

  @Override
  public Row next() throws FileStoreException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Row row = nextRow;
    try {
      nextRow = fetchNext();
    } catch (IOException e) {
      throw new FileStoreException(e);
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
    Object[] values = new Object[]{data};
    return new Row(header, currentKey, values);
  }
}
