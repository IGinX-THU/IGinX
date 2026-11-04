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
package cn.edu.tsinghua.iginx.minio.struct;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import javax.annotation.Nullable;

/** Columns and keys of data, the column interval is [start, end) and null means unbounded. */
public final class DataBoundary {

  private static final DataBoundary EMPTY = new DataBoundary(null, null, false);

  @Nullable private final String startColumn;
  @Nullable private final String endColumn;
  private final boolean hasData;

  private DataBoundary(@Nullable String startColumn, @Nullable String endColumn, boolean hasData) {
    this.startColumn = startColumn;
    this.endColumn = endColumn;
    this.hasData = hasData;
  }

  public static DataBoundary empty() {
    return EMPTY;
  }

  /** boundary of columns in [startColumn, endColumn) with all keys */
  public static DataBoundary of(@Nullable String startColumn, @Nullable String endColumn) {
    return new DataBoundary(startColumn, endColumn, true);
  }

  public boolean isEmpty() {
    return !hasData;
  }

  public ColumnsInterval getColumnsInterval() {
    return new ColumnsInterval(startColumn, endColumn);
  }

  public KeyInterval getKeyInterval() {
    return KeyInterval.getDefaultKeyInterval();
  }

  @Override
  public String toString() {
    return hasData ? "DataBoundary[" + startColumn + ", " + endColumn + ")" : "DataBoundary{}";
  }
}
