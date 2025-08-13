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
package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.InFilter.InOp.getDeMorganOpposite;
import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.InFilter.InOp.getOppositeOp;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class InFilter implements Filter {

  String path;

  InOp inOp;

  public enum InOp {
    IN_AND,
    IN_OR,
    NOT_IN_AND,
    NOT_IN_OR;

    public boolean isOrOp() {
      return this == IN_OR || this == NOT_IN_OR;
    }

    public boolean isNotOp() {
      return this == NOT_IN_AND || this == NOT_IN_OR;
    }

    public static InOp getOppositeOp(InOp inOp) {
      switch (inOp) {
        case IN_AND:
          return NOT_IN_AND;
        case IN_OR:
          return NOT_IN_OR;
        case NOT_IN_AND:
          return IN_AND;
        case NOT_IN_OR:
          return IN_OR;
        default:
          return inOp;
      }
    }

    public static InOp getDeMorganOpposite(InOp inOp) {
      switch (inOp) {
        case IN_AND:
          return NOT_IN_OR;
        case IN_OR:
          return NOT_IN_AND;
        case NOT_IN_AND:
          return IN_OR;
        case NOT_IN_OR:
          return IN_AND;
        default:
          return inOp;
      }
    }

    public static InOp str2Op(String str) {
      switch (str.toLowerCase()) {
        case "|in":
        case "in":
          return IN_OR;
        case "|not in":
        case "not in":
          return NOT_IN_OR;
        case "&in":
          return IN_AND;
        case "&not in":
          return NOT_IN_AND;
        default:
          throw new SQLParserException("Unsupported InOp: " + str);
      }
    }

    @Override
    public String toString() {
      switch (this) {
        case IN_AND:
          return "&in";
        case IN_OR:
          return "in";
        case NOT_IN_AND:
          return "&not in";
        case NOT_IN_OR:
          return "not in";
        default:
          throw new SQLParserException("Unsupported InOp: " + this);
      }
    }
  }

  Set<Value> values = new HashSet<>();

  public InFilter(String path, InOp inOp, Collection<Value> values) {
    this.path = path;
    this.values.addAll(values);
    this.inOp = inOp;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Set<Value> getValues() {
    return values;
  }

  public InOp getInOp() {
    return inOp;
  }

  public void setInOp(InOp inOp) {
    this.inOp = inOp;
  }

  public void reverseFunc() {
    if (path.contains("*")) {
      this.inOp = getDeMorganOpposite(inOp);
    } else {
      this.inOp = getOppositeOp(inOp);
    }
  }

  public boolean validate(Value value) {
    return values.contains(value) ^ inOp.isNotOp();
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public FilterType getType() {
    return FilterType.In;
  }

  @Override
  public Filter copy() {
    return new InFilter(path, inOp, new HashSet<>(values));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InFilter inFilter = (InFilter) o;
    return Objects.equals(path, inFilter.path)
        && inOp == inFilter.inOp
        && Objects.equals(values, inFilter.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, inOp, values);
  }

  @Override
  public String toString() {
    return String.format(
        "%s %s (%s)", path, inOp, values.toString().substring(1, values.toString().length() - 1));
  }
}
