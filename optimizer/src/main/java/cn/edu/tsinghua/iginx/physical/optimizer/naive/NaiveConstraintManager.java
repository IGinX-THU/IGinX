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
package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import java.util.List;

class NaiveConstraintManager implements ConstraintManager {

  private static final NaiveConstraintManager INSTANCE = new NaiveConstraintManager();

  private NaiveConstraintManager() {}

  public static NaiveConstraintManager getInstance() {
    return INSTANCE;
  }

  private boolean checkOperator(Operator operator) {
    if (OperatorType.isBinaryOperator(operator.getType())) {
      return checkBinaryOperator((BinaryOperator) operator);
    }
    if (OperatorType.isUnaryOperator(operator.getType())) {
      return checkUnaryOperator((UnaryOperator) operator);
    }
    if (OperatorType.isMultipleOperator(operator.getType())) {
      return checkMultipleOperator((MultipleOperator) operator);
    }
    return OperatorType.isGlobalOperator(operator.getType()); // 未能识别的操作符
  }

  @Override
  public boolean check(Operator root) {
    if (root == null) {
      return false;
    }
    return checkOperator(root);
  }

  private boolean checkBinaryOperator(BinaryOperator binaryOperator) {
    Source sourceA = binaryOperator.getSourceA();
    Source sourceB = binaryOperator.getSourceB();
    if (sourceA == null || sourceB == null) {
      return false;
    }
    if (sourceA.getType() == SourceType.Fragment
        || sourceB.getType() == SourceType.Fragment) { // binary 的操作符的来源应该均为别的操作符的输出
      return false;
    }
    Operator sourceOperatorA = ((OperatorSource) sourceA).getOperator();
    Operator sourceOperatorB = ((OperatorSource) sourceB).getOperator();
    return checkOperator(sourceOperatorA) && checkOperator(sourceOperatorB);
  }

  private boolean checkUnaryOperator(UnaryOperator unaryOperator) {
    Source source = unaryOperator.getSource();
    if (source == null) {
      return false;
    }
    if (source.getType() == SourceType.Fragment) {
      return unaryOperator.getType() == OperatorType.Project
          || unaryOperator.getType() == OperatorType.Delete
          || unaryOperator.getType() == OperatorType.Insert;
    }
    Operator sourceOperator = ((OperatorSource) source).getOperator();
    return checkOperator(sourceOperator);
  }

  public boolean checkMultipleOperator(MultipleOperator multipleOperator) {
    List<Source> sources = multipleOperator.getSources();
    for (Source source : sources) {
      if (source.getType() == SourceType.Fragment) {
        return false;
      }
      Operator sourceOperator = ((OperatorSource) source).getOperator();
      if (!checkOperator(sourceOperator)) {
        return false;
      }
    }
    return true;
  }
}
