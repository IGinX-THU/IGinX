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

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Objects;

public class ValueFilter implements Filter {

  private final FilterType type = FilterType.Value;

  private String path;
  private final Value value;
  private Op op;

  public ValueFilter(String path, Op op, Value value) {
    this.path = path;
    this.op = op;
    this.value = value;
  }

  public void reverseFunc() {
    this.op = path.contains("*") ? Op.getDeMorganOpposite(op) : Op.getOpposite(op);
  }

  public String getPath() {
    return path;
  }

  public String setPath(String path) {
    return this.path = path;
  }

  public Op getOp() {
    return op;
  }

  public Value getValue() {
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
    return new ValueFilter(path, op, value.copy());
  }

  @Override
  public String toString() {
    Object valueObj =
        value.getDataType() == DataType.BINARY
            ? "\"" + value.getBinaryVAsString() + "\""
            : value.getValue();
    return path + " " + Op.op2Str(op) + " " + valueObj;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValueFilter that = (ValueFilter) o;
    return type == that.type
        && Objects.equals(path, that.path)
        && Objects.equals(value, that.value)
        && op == that.op;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, path, value, op);
  }
}
