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
package cn.edu.tsinghua.iginx.s3.common;

import java.time.Instant;
import java.util.Objects;
import javax.annotation.Nullable;

/** An object or a common prefix (directory) returned by {@link S3Client}. */
public final class S3Object {

  private final String key;
  private final long size;
  @Nullable private final Instant lastModified;
  private final boolean dir;

  S3Object(String key, long size, @Nullable Instant lastModified, boolean dir) {
    this.key = Objects.requireNonNull(key);
    this.size = size;
    this.lastModified = lastModified;
    this.dir = dir;
  }

  /** full key in the bucket, ends with "/" for a directory */
  public String key() {
    return key;
  }

  public long size() {
    return size;
  }

  @Nullable
  public Instant lastModified() {
    return lastModified;
  }

  public boolean isDir() {
    return dir;
  }
}
