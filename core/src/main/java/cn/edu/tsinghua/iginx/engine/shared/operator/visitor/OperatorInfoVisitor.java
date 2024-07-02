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
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.ArrayList;
import java.util.List;

public class OperatorInfoVisitor implements OperatorVisitor {

  private final List<Object[]> cache = new ArrayList<>();

  private int maxLen = 0;

  private int depth = -1;

  public List<Object[]> getCache() {
    return cache;
  }

  public int getMaxLen() {
    return maxLen;
  }

  @Override
  public void enter() {
    depth++;
  }

  @Override
  public void leave() {
    depth--;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    collectOperatorInfo(unaryOperator);
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    collectOperatorInfo(binaryOperator);
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    collectOperatorInfo(multipleOperator);
  }

  private void collectOperatorInfo(Operator op) {
    OperatorType type = op.getType();
    StringBuilder builder = new StringBuilder();
    if (depth != 0) {
      for (int i = 0; i < depth; i++) {
        builder.append("  ");
      }
      builder.append("+--");
    }
    builder.append(type);

    maxLen = Math.max(maxLen, builder.length());

    Object[] values = new Object[3];
    values[0] = builder.toString();
    values[1] = op.getType().toString().getBytes();
    values[2] = op.getInfo().getBytes();
    cache.add(values);
  }
}
