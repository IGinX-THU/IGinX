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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class ValueToSelectedPath extends AbstractUnaryOperator {

  private final String prefix;

  public ValueToSelectedPath(Source source, String prefix) {
    super(OperatorType.ValueToSelectedPath, source);
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  @Override
  public Operator copy() {
    return new ValueToSelectedPath(getSource().copy(), prefix);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new ValueToSelectedPath(source, prefix);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    if (prefix != null && !prefix.isEmpty()) {
      builder.append("prefix: ");
      builder.append(prefix);
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
    ValueToSelectedPath that = (ValueToSelectedPath) object;
    return prefix.equals(that.prefix);
  }
}
