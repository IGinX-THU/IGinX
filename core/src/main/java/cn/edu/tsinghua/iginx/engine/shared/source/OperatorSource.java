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
package cn.edu.tsinghua.iginx.engine.shared.source;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class OperatorSource extends AbstractSource {

  private final Operator operator;

  public OperatorSource(Operator operator) {
    super(SourceType.Operator);
    if (operator == null) {
      throw new IllegalArgumentException("operator shouldn't be null");
    }
    this.operator = operator;
  }

  public Operator getOperator() {
    return operator;
  }

  @Override
  public Source copy() {
    return new OperatorSource(operator.copy());
  }
}
