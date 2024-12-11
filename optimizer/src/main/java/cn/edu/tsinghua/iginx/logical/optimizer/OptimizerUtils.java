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
package cn.edu.tsinghua.iginx.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptimizerUtils {
  public static boolean validateAggPushDown(Operator operator) {
    if (operator == null) {
      return false;
    }
    if (operator.getType() != OperatorType.GroupBy
        && operator.getType() != OperatorType.SetTransform) {
      return false;
    }

    List<FunctionCall> functionCallList = new ArrayList<>();
    List<Expression> expressions = new ArrayList<>();

    if (operator.getType() == OperatorType.GroupBy) {
      GroupBy groupBy = (GroupBy) operator;
      functionCallList = groupBy.getFunctionCallList();
      expressions.addAll(groupBy.getGroupByExpressions());
    } else {
      // SetTransform
      SetTransform setTransform = (SetTransform) operator;
      functionCallList = setTransform.getFunctionCallList();
    }

    for (FunctionCall fc : functionCallList) {
      if (!Arrays.asList("AVG", "MAX", "MIN", "SUM", "COUNT")
          .contains(fc.getFunction().getIdentifier().toUpperCase())) {
        return false;
      }
      if (fc.getParams().isDistinct()) {
        return false;
      }
      expressions.addAll(fc.getParams().getExpressions());
    }

    for (Expression expression : expressions) {
      final boolean[] isValid = {true};
      expression.accept(
          new ExpressionVisitor() {
            @Override
            public void visit(BaseExpression expression) {}

            @Override
            public void visit(BinaryExpression expression) {}

            @Override
            public void visit(BracketExpression expression) {}

            @Override
            public void visit(ConstantExpression expression) {}

            @Override
            public void visit(FromValueExpression expression) {
              isValid[0] = false;
            }

            @Override
            public void visit(FuncExpression expression) {}

            @Override
            public void visit(MultipleExpression expression) {}

            @Override
            public void visit(UnaryExpression expression) {}

            @Override
            public void visit(CaseWhenExpression expression) {
              isValid[0] = false;
            }

            @Override
            public void visit(KeyExpression expression) {
              isValid[0] = false;
            }

            @Override
            public void visit(SequenceExpression expression) {
              isValid[0] = false;
            }
          });
      if (!isValid[0]) {
        return false;
      }
    }

    return true;
  }
}
