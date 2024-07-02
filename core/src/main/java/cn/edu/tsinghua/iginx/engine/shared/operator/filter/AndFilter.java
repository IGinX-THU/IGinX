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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AndFilter implements Filter {

  private final FilterType type = FilterType.And;

  private final List<Filter> children;

  public AndFilter(List<Filter> children) {
    this.children = children;
  }

  public List<Filter> getChildren() {
    return children;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
    this.children.forEach(child -> child.accept(visitor));
  }

  @Override
  public FilterType getType() {
    return type;
  }

  public void setChildren(List<Filter> children) {
    this.children.clear();
    this.children.addAll(children);
  }

  @Override
  public Filter copy() {
    List<Filter> newChildren = new ArrayList<>();
    children.forEach(e -> newChildren.add(e.copy()));
    return new AndFilter(newChildren);
  }

  @Override
  public String toString() {
    return children.stream().map(Object::toString).collect(Collectors.joining(" && ", "(", ")"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AndFilter andFilter = (AndFilter) o;
    return type == andFilter.type && Objects.equals(children, andFilter.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, children);
  }
}
