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
package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PreciseTagFilter implements TagFilter {

  private final List<BasePreciseTagFilter> children;

  public PreciseTagFilter(List<BasePreciseTagFilter> children) {
    this.children = children;
  }

  public List<BasePreciseTagFilter> getChildren() {
    return children;
  }

  @Override
  public TagFilterType getType() {
    return TagFilterType.Precise;
  }

  @Override
  public TagFilter copy() {
    List<BasePreciseTagFilter> newChildren = new ArrayList<>();
    children.forEach(e -> newChildren.add((BasePreciseTagFilter) e.copy()));
    return new PreciseTagFilter(newChildren);
  }

  @Override
  public String toString() {
    return children.stream().map(Object::toString).collect(Collectors.joining(" || ", "(", ")"));
  }
}
