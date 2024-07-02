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

// only use for calculate sub filter for now.
public class BoolFilter implements Filter {

  private final FilterType type = FilterType.Bool;

  private final boolean isTrue;

  public BoolFilter(boolean isTrue) {
    this.isTrue = isTrue;
  }

  public boolean isTrue() {
    return isTrue;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public FilterType getType() {
    return type;
  }

  @Override
  public Filter copy() {
    return new BoolFilter(isTrue);
  }

  @Override
  public String toString() {
    return isTrue ? "True" : "False";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BoolFilter that = (BoolFilter) o;
    return isTrue == that.isTrue && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, isTrue);
  }
}
