/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import java.util.HashSet;
import java.util.Set;

public class ShowColumns extends AbstractUnaryOperator {

  private final Set<String> pathRegexSet;
  private final TagFilter tagFilter;
  private final int limit;
  private final int offset;

  public ShowColumns(
      GlobalSource source, Set<String> pathRegexSet, TagFilter tagFilter, int limit, int offset) {
    super(OperatorType.ShowColumns, source);
    this.pathRegexSet = pathRegexSet;
    this.tagFilter = tagFilter;
    this.limit = limit;
    this.offset = offset;
  }

  public ShowColumns(GlobalSource source, ShowColumnsStatement statement) {
    this(
        source,
        statement.getPathRegexSet(),
        statement.getTagFilter(),
        statement.getLimit(),
        statement.getOffset());
  }

  public Set<String> getPathRegexSet() {
    return pathRegexSet;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public Operator copy() {
    return new ShowColumns(
        (GlobalSource) getSource().copy(),
        new HashSet<>(pathRegexSet),
        tagFilter.copy(),
        limit,
        offset);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new ShowColumns(
        (GlobalSource) source, new HashSet<>(pathRegexSet), tagFilter.copy(), limit, offset);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    if (pathRegexSet != null && !pathRegexSet.isEmpty()) {
      builder.append("Patterns: ");
      for (String regex : pathRegexSet) {
        builder.append(regex).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    if (tagFilter != null) {
      builder.append(", TagFilter: ").append(tagFilter);
    }
    if (limit != Integer.MAX_VALUE || offset != 0) {
      builder.append(", Limit: ").append(limit).append(", Offset: ").append(offset);
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    ShowColumns that = (ShowColumns) object;
    return limit == that.limit
        && offset == that.offset
        && pathRegexSet.equals(that.pathRegexSet)
        && tagFilter.equals(that.tagFilter);
  }
}
