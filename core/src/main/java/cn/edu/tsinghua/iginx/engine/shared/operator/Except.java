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
import java.util.ArrayList;
import java.util.List;

public class Except extends AbstractBinaryOperator {

  private final List<String> leftOrder;

  private final List<String> rightOrder;

  private final boolean isDistinct;

  public Except(
      Source sourceA,
      Source sourceB,
      List<String> leftOrder,
      List<String> rightOrder,
      boolean isDistinct) {
    super(OperatorType.Except, sourceA, sourceB);
    this.leftOrder = leftOrder;
    this.rightOrder = rightOrder;
    this.isDistinct = isDistinct;
  }

  public List<String> getLeftOrder() {
    return leftOrder;
  }

  public List<String> getRightOrder() {
    return rightOrder;
  }

  public void setLeftOrder(List<String> leftOrder) {
    this.leftOrder.clear();
    this.leftOrder.addAll(leftOrder);
  }

  public void setRightOrder(List<String> rightOrder) {
    this.rightOrder.clear();
    this.rightOrder.addAll(rightOrder);
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public Operator copy() {
    return new Except(
        getSourceA().copy(),
        getSourceB().copy(),
        new ArrayList<>(leftOrder),
        new ArrayList<>(rightOrder),
        isDistinct);
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new Except(
        sourceA, sourceB, new ArrayList<>(leftOrder), new ArrayList<>(rightOrder), isDistinct);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("LeftOrder: ");
    for (String order : leftOrder) {
      builder.append(order).append(",");
    }
    builder.append(" RightOrder: ");
    for (String order : rightOrder) {
      builder.append(order).append(",");
    }
    builder.append(" isDistinct: ").append(isDistinct);
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
    Except that = (Except) object;
    return leftOrder.equals(that.leftOrder)
        && rightOrder.equals(that.rightOrder)
        && isDistinct == that.isDistinct;
  }
}
