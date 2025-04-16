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

public class Load extends AbstractUnaryOperator {

  private final Operator operator;

  private final int index;

  public Load(Source source, int index, Operator operator) {
    super(OperatorType.Load, source);
    this.index = index;
    this.operator = operator;
  }

  public Operator getOperator() {
    return operator;
  }

  @Override
  public Operator copy() {
    return new Load(getSource().copy(), index, operator.copy());
  }

  @Override
  public String getInfo() {
    return "Load" + index + ": " + getSource().toString();
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Load(source, index, operator.copy());
  }
}
