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
package cn.edu.tsinghua.iginx.sql.utils;

import cn.edu.tsinghua.iginx.engine.shared.expr.BinaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BracketExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.MultipleExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Operator;
import cn.edu.tsinghua.iginx.engine.shared.expr.UnaryExpression;

public class ExpressionUtils {

  public static boolean isConstantArithmeticExpr(Expression expression) {
    switch (expression.getType()) {
      case Constant:
        return true;
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expression;
        return isConstantArithmeticExpr(bracketExpression.getExpression());
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expression;
        return isConstantArithmeticExpr(unaryExpression.getExpression());
      case Binary:
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        return isConstantArithmeticExpr(binaryExpression.getLeftExpression())
            && isConstantArithmeticExpr(binaryExpression.getRightExpression());
      case Function:
        FuncExpression funcExpression = (FuncExpression) expression;
        return funcExpression.getExpressions().stream()
            .allMatch(ExpressionUtils::isConstantArithmeticExpr);
      case Multiple:
        MultipleExpression multipleExpression = (MultipleExpression) expression;
        return multipleExpression.getChildren().stream()
            .allMatch(ExpressionUtils::isConstantArithmeticExpr);
      default:
        return false;
    }
  }

  public static String transformToBaseExpr(Expression expression) {
    switch (expression.getType()) {
      case Base:
        return expression.getColumnName();
      case Bracket:
        BracketExpression bracketExpression = (BracketExpression) expression;
        return transformToBaseExpr(bracketExpression.getExpression());
      case Unary:
        UnaryExpression unaryExpression = (UnaryExpression) expression;
        if (unaryExpression.getOperator().equals(Operator.PLUS)) {
          return transformToBaseExpr(unaryExpression.getExpression());
        } else {
          return null;
        }
      default:
        return null;
    }
  }
}
