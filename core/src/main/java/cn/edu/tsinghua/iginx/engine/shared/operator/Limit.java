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

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Limit extends AbstractUnaryOperator {

  private final int limit;

  private final int offset;

  public Limit(Source source, int limit, int offset) {
    super(OperatorType.Limit, source);
    if (limit < 0 || offset < 0) {
      throw new IllegalArgumentException("limit and offset shouldn't less than zero");
    }
    this.limit = limit;
    this.offset = offset;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public Operator copy() {
    return new Limit(getSource().copy(), limit, offset);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Limit(source, limit, offset);
  }

  @Override
  public String getInfo() {
    return "Limit: " + limit + ", Offset: " + offset;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Limit limit1 = (Limit) object;
    return limit == limit1.limit && offset == limit1.offset;
  }
}
