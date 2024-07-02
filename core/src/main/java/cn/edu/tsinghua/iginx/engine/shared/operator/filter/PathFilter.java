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

public class PathFilter implements Filter {

  private final FilterType type = FilterType.Path;

  private String pathA;
  private String pathB;
  private Op op;

  public PathFilter(String pathA, Op op, String pathB) {
    this.pathA = pathA;
    this.pathB = pathB;
    this.op = op;
  }

  public void reverseFunc() {
    this.op = Op.getOpposite(op);
  }

  public String getPathA() {
    return pathA;
  }

  public String getPathB() {
    return pathB;
  }

  public String setPathA(String pathA) {
    return this.pathA = pathA;
  }

  public String setPathB(String pathB) {
    return this.pathB = pathB;
  }

  public Op getOp() {
    return op;
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
    return new PathFilter(pathA, op, pathB);
  }

  @Override
  public String toString() {
    return pathA + " " + Op.op2Str(op) + " " + pathB;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathFilter that = (PathFilter) o;
    return type == that.type
        && Objects.equals(pathA, that.pathA)
        && Objects.equals(pathB, that.pathB)
        && op == that.op;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, pathA, pathB, op);
  }
}
