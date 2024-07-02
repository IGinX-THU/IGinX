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
package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public class TreeInfoVisitor implements OperatorVisitor {

  private final StringBuilder builder = new StringBuilder();

  private int spacer = -2;

  @Override
  public void enter() {
    spacer += 2;
  }

  @Override
  public void leave() {
    spacer -= 2;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    makeRoomForSpacer();
    builder
        .append("[")
        .append(unaryOperator.getType())
        .append("] ")
        .append(unaryOperator.getInfo())
        .append("\n");
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    makeRoomForSpacer();
    builder
        .append("[")
        .append(binaryOperator.getType())
        .append("] ")
        .append(binaryOperator.getInfo())
        .append("\n");
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    makeRoomForSpacer();
    builder
        .append("[")
        .append(multipleOperator.getType())
        .append("] ")
        .append(multipleOperator.getInfo())
        .append("\n");
  }

  private void makeRoomForSpacer() {
    for (int i = 0; i < spacer; i++) {
      builder.append(" ");
    }
  }

  public String getTreeInfo() {
    return builder.toString();
  }
}
