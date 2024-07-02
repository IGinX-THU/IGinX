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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Select extends AbstractUnaryOperator {

  private Filter filter;

  private TagFilter tagFilter;

  public Select(Source source, Filter filter, TagFilter tagFilter) {
    super(OperatorType.Select, source);
    if (filter == null) {
      throw new IllegalArgumentException("filter shouldn't be null");
    }
    this.filter = filter;
    this.tagFilter = tagFilter;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  @Override
  public Operator copy() {
    return new Select(
        getSource().copy(), filter.copy(), tagFilter == null ? null : tagFilter.copy());
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Select(source, filter.copy(), tagFilter == null ? null : tagFilter.copy());
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Filter: ").append(filter.toString());
    if (tagFilter != null) {
      builder.append(", TagFilter: ").append(tagFilter.toString());
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
    if (!super.equals(object)) {
      return false;
    }
    Select select = (Select) object;
    return filter.equals(select.filter) && tagFilter.equals(select.tagFilter);
  }
}
