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
package cn.edu.tsinghua.iginx.minio.kv;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * File name of a segment object: {@code <id>_<TYPE>_<minKey>_<maxKey>.seg}.
 *
 * <p>{@code id} is {@code <13 digits millis>-<6 digits sequence>-<8 hex random>}, so that ids
 * compare in write order (within one process) and never collide between processes. When segments of
 * a column contain the same key, the one with the greater id wins.
 */
public final class SegmentName implements Comparable<SegmentName> {

  public static final String SUFFIX = ".seg";
  private static final String SEPARATOR = "_";
  private static final AtomicLong SEQUENCE = new AtomicLong();

  private final String id;
  private final DataType type;
  private final long minKey;
  private final long maxKey;

  public SegmentName(String id, DataType type, long minKey, long maxKey) {
    this.id = Objects.requireNonNull(id);
    this.type = Objects.requireNonNull(type);
    this.minKey = minKey;
    this.maxKey = maxKey;
  }

  public static String newId() {
    return String.format(
        "%013d-%06d-%08x",
        System.currentTimeMillis(),
        SEQUENCE.getAndIncrement() % 1_000_000,
        ThreadLocalRandom.current().nextInt());
  }

  @Nullable
  public static SegmentName parse(String fileName) {
    if (!fileName.endsWith(SUFFIX)) {
      return null;
    }
    String[] parts = fileName.substring(0, fileName.length() - SUFFIX.length()).split(SEPARATOR);
    if (parts.length != 4) {
      return null;
    }
    try {
      DataType type = DataType.valueOf(parts[1]);
      return new SegmentName(parts[0], type, Long.parseLong(parts[2]), Long.parseLong(parts[3]));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public String getId() {
    return id;
  }

  public DataType getType() {
    return type;
  }

  public long getMinKey() {
    return minKey;
  }

  public long getMaxKey() {
    return maxKey;
  }

  public String toFileName() {
    return String.join(SEPARATOR, id, type.name(), Long.toString(minKey), Long.toString(maxKey))
        + SUFFIX;
  }

  @Override
  public int compareTo(SegmentName o) {
    return id.compareTo(o.id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentName)) {
      return false;
    }
    SegmentName that = (SegmentName) o;
    return minKey == that.minKey
        && maxKey == that.maxKey
        && id.equals(that.id)
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, minKey, maxKey);
  }

  @Override
  public String toString() {
    return toFileName();
  }
}
