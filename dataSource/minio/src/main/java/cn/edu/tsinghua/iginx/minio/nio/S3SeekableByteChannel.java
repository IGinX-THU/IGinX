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
package cn.edu.tsinghua.iginx.minio.nio;

import cn.edu.tsinghua.iginx.minio.common.MinioCalls;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

/**
 * Read-only channel over one object. Data is fetched with ranged GET requests in blocks, and the
 * last fetched block is kept so that small sequential reads do not issue a request each.
 */
final class S3SeekableByteChannel implements SeekableByteChannel {

  private final S3FileSystem fs;
  private final String bucket;
  private final String key;
  private final long size;

  private long position = 0;
  private boolean open = true;

  private byte[] block = new byte[0];
  private long blockStart = 0;

  S3SeekableByteChannel(S3FileSystem fs, String bucket, String key, long size) {
    this.fs = fs;
    this.bucket = bucket;
    this.key = key;
    this.size = size;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    ensureOpen();
    if (position >= size) {
      return -1;
    }
    int total = 0;
    while (dst.hasRemaining() && position < size) {
      if (position < blockStart || position >= blockStart + block.length) {
        long length = Math.min(Math.max(fs.blockSize(), dst.remaining()), size - position);
        fetch(position, length);
      }
      int offset = (int) (position - blockStart);
      int n = Math.min(dst.remaining(), block.length - offset);
      dst.put(block, offset, n);
      position += n;
      total += n;
    }
    return total;
  }

  private void fetch(long offset, long length) throws IOException {
    byte[] buffer = new byte[Math.toIntExact(length)];
    try (GetObjectResponse response =
        MinioCalls.call(
            () ->
                fs.client()
                    .getObject(
                        GetObjectArgs.builder()
                            .bucket(bucket)
                            .object(key)
                            .offset(offset)
                            .length(length)
                            .build()))) {
      int read = 0;
      while (read < buffer.length) {
        int n = response.read(buffer, read, buffer.length - read);
        if (n < 0) {
          break;
        }
        read += n;
      }
      if (read < buffer.length) {
        byte[] truncated = new byte[read];
        System.arraycopy(buffer, 0, truncated, 0, read);
        buffer = truncated;
      }
    }
    if (buffer.length == 0) {
      throw new IOException("unexpected end of object " + bucket + "/" + key + " at " + offset);
    }
    this.block = buffer;
    this.blockStart = offset;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new NonWritableChannelException();
  }

  @Override
  public long position() throws IOException {
    ensureOpen();
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    ensureOpen();
    if (newPosition < 0) {
      throw new IllegalArgumentException("negative position: " + newPosition);
    }
    this.position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    ensureOpen();
    return size;
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new NonWritableChannelException();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    open = false;
    block = new byte[0];
  }

  private void ensureOpen() throws ClosedChannelException {
    if (!open) {
      throw new ClosedChannelException();
    }
  }
}
