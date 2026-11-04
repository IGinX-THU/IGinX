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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import java.util.List;
import javax.annotation.Nullable;

/** Rows and columns to read or delete. */
public final class DataTarget {

  @Nullable private final Filter filter;
  @Nullable private final List<String> patterns;
  @Nullable private final TagFilter tagFilter;

  /**
   * @param filter filter of rows, null means all rows
   * @param patterns patterns of columns, null means all columns, empty means no column
   * @param tagFilter filter of tags, null means all tags
   */
  public DataTarget(
      @Nullable Filter filter, @Nullable List<String> patterns, @Nullable TagFilter tagFilter) {
    this.filter = filter;
    this.patterns = patterns;
    this.tagFilter = tagFilter;
  }

  @Nullable
  public Filter getFilter() {
    return filter;
  }

  @Nullable
  public List<String> getPatterns() {
    return patterns;
  }

  @Nullable
  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public DataTarget withPatterns(@Nullable List<String> patterns) {
    return new DataTarget(filter, patterns, tagFilter);
  }

  @Override
  public String toString() {
    return "DataTarget{filter="
        + filter
        + ", patterns="
        + patterns
        + ", tagFilter="
        + tagFilter
        + '}';
  }
}
