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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BinaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BracketExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.MultipleExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Operator;
import cn.edu.tsinghua.iginx.engine.shared.expr.UnaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  /**
   * 从expressions中获取所有的BaseExpression
   *
   * @param expressions Expression列表
   * @param exceptFunc 是否不包括FuncExpression参数中的BaseExpression
   * @return BaseExpression列表
   */
  public static List<BaseExpression> getBaseExpressionList(
      List<Expression> expressions, boolean exceptFunc) {
    List<String> paths = ExprUtils.getPathFromExprList(expressions, exceptFunc);
    List<BaseExpression> baseExpressionList = new ArrayList<>(paths.size());
    for (String path : paths) {
      baseExpressionList.add(new BaseExpression(path));
    }
    return baseExpressionList;
  }

  /**
   * 判断Expression的FuncExpression的映射类型
   *
   * @param expression 给定Expression
   * @return Expression的函数映射类型。若为ConstantExpression，返回null
   */
  public static MappingType getExprMappingType(Expression expression) {
    switch (expression.getType()) {
      case Constant:
      case Sequence:
      case FromValue:
        return null;
      case Base:
      case Key:
      case CaseWhen:
        return MappingType.RowMapping; // case-when视为RowMapping函数
      case Unary:
        return getExprMappingType(((UnaryExpression) expression).getExpression());
      case Bracket:
        return getExprMappingType(((BracketExpression) expression).getExpression());
      case Function:
        FuncExpression funcExpr = (FuncExpression) expression;
        MappingType funcMappingType = FunctionUtils.getFunctionMappingType(funcExpr.getFuncName());
        Set<MappingType> childTypeSet = new HashSet<>();
        MappingType retType = funcMappingType;
        for (Expression child : funcExpr.getExpressions()) {
          MappingType childType = getExprMappingType(child);
          childTypeSet.add(childType);
          if (funcMappingType == MappingType.SetMapping) {
            if (childType != null && childType != MappingType.RowMapping) {
              throw new SQLParserException(
                  "SetToRow functions can not be nested with SetToSet/SetToRow functions.");
            }
          } else if (funcMappingType == MappingType.Mapping) {
            if (childType != null && childType != MappingType.RowMapping) {
              throw new SQLParserException(
                  "SetToSet functions can not be nested with SetToSet/SetToRow functions.");
            }
          } else {
            if (childType != null) {
              retType = childType;
            }
          }
        }
        childTypeSet.remove(null);
        if (childTypeSet.size() > 1) {
          throw new SQLParserException(
              "SetToSet/SetToRow/RowToRow functions can not be mixed in function params.");
        }
        return retType;
      case Binary:
        BinaryExpression binaryExpr = (BinaryExpression) expression;
        MappingType leftType = getExprMappingType(binaryExpr.getLeftExpression());
        MappingType rightType = getExprMappingType(binaryExpr.getRightExpression());
        if (leftType != null && rightType != null) {
          if (leftType != rightType) {
            throw new SQLParserException(
                "SetToSet/SetToRow/RowToRow functions can not be mixed in BinaryExpression.");
          }
          return leftType;
        }
        if (leftType == null) {
          return rightType;
        }
        return leftType;
      case Multiple:
        MultipleExpression multipleExpr = (MultipleExpression) expression;
        Set<MappingType> typeSet = new HashSet<>();
        for (Expression child : multipleExpr.getChildren()) {
          MappingType childType = getExprMappingType(child);
          if (childType != null) {
            typeSet.add(childType);
          }
        }
        if (typeSet.size() == 1) {
          return typeSet.iterator().next();
        } else if (typeSet.size() > 1) {
          throw new SQLParserException(
              "SetToSet/SetToRow/RowToRow functions can not be mixed in MultipleExpression.");
        } else {
          return null;
        }
      default:
        throw new SQLParserException("Unknown expression type: " + expression.getType());
    }
  }
}
