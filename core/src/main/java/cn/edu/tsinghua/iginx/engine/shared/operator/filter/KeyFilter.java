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

public class KeyFilter implements Filter {

  private final FilterType type = FilterType.Key;

  private final long value;
  private Op op;

  public KeyFilter(Op op, long value) {
    this.op = op;
    this.value = value;
  }

  public void reverseFunc() {
    this.op = Op.getOpposite(op);
  }

  public Op getOp() {
    return op;
  }

  public long getValue() {
    return value;
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
    return new KeyFilter(op, value);
  }

  @Override
  public String toString() {
    return "key " + Op.op2Str(op) + " " + value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyFilter that = (KeyFilter) o;
    return value == that.value && type == that.type && op == that.op;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value, op);
  }
}
