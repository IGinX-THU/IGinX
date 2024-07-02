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
package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.Objects;

public class NotFilter implements Filter {

  private final FilterType type = FilterType.Not;

  private Filter child;

  public NotFilter(Filter child) {
    this.child = child;
  }

  public Filter getChild() {
    return child;
  }

  public void setChild(Filter child) {
    this.child = child;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
    this.child.accept(visitor);
  }

  @Override
  public FilterType getType() {
    return type;
  }

  @Override
  public Filter copy() {
    return new NotFilter(child.copy());
  }

  @Override
  public String toString() {
    return "!(" + child.toString() + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NotFilter notFilter = (NotFilter) o;
    return type == notFilter.type && Objects.equals(child, notFilter.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, child);
  }
}
