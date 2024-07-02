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

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Join extends AbstractBinaryOperator {

  private final String joinBy;

  public Join(Source sourceA, Source sourceB) {
    this(sourceA, sourceB, Constants.KEY);
  }

  public Join(Source sourceA, Source sourceB, String joinBy) {
    super(OperatorType.Join, sourceA, sourceB);
    if (joinBy == null) {
      throw new IllegalArgumentException("joinBy shouldn't be null");
    }
    this.joinBy = joinBy;
  }

  public String getJoinBy() {
    return joinBy;
  }

  @Override
  public Operator copy() {
    return new Join(getSourceA().copy(), getSourceB().copy(), joinBy);
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new Join(sourceA, sourceB, joinBy);
  }

  @Override
  public String getInfo() {
    return "JoinBy: " + joinBy;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Join join = (Join) object;
    return joinBy.equals(join.joinBy);
  }
}
