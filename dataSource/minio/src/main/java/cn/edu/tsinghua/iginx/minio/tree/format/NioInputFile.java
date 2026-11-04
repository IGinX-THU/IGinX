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

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import shaded.iginx.org.apache.parquet.io.DelegatingSeekableInputStream;
import shaded.iginx.org.apache.parquet.io.InputFile;
import shaded.iginx.org.apache.parquet.io.SeekableInputStream;

/** Parquet input of a {@link Path} on any file system, such as the S3 one. */
final class NioInputFile implements InputFile {

  private final Path path;

  NioInputFile(Path path) {
    this.path = Objects.requireNonNull(path);
  }

  @Override
  public long getLength() throws IOException {
    return Files.size(path);
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    SeekableByteChannel channel = Files.newByteChannel(path);
    return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {
      @Override
      public long getPos() throws IOException {
        return channel.position();
      }

      @Override
      public void seek(long newPos) throws IOException {
        channel.position(newPos);
      }
    };
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
