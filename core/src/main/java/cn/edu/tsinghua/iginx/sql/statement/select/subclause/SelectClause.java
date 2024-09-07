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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.engine.shared.expr.BinaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BracketExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.MultipleExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.UnaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.*;

public class SelectClause {
  private final List<SubQueryFromPart> selectSubQueryParts;
  private boolean isDistinct = false;
  private boolean hasValueToSelectedPath = false;
  private final List<Expression> expressions;
  private final Set<String> pathSet;

  public SelectClause() {
    this.selectSubQueryParts = new ArrayList<>();
    this.expressions = new ArrayList<>();
    this.pathSet = new HashSet<>();
  }

  public void addSelectSubQueryPart(SubQueryFromPart selectSubQueryPart) {
    this.selectSubQueryParts.add(selectSubQueryPart);
  }

  public List<SubQueryFromPart> getSelectSubQueryParts() {
    return selectSubQueryParts;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean distinct) {
    isDistinct = distinct;
  }

  public boolean hasValueToSelectedPath() {
    return hasValueToSelectedPath;
  }

  public void setHasValueToSelectedPath(boolean hasValueToSelectedPath) {
    this.hasValueToSelectedPath = hasValueToSelectedPath;
  }

  public boolean isLastFirst() {
    List<FuncExpression> funcList = getTargetTypeFuncExprList(MappingType.Mapping);
    boolean isLastFirst = true;
    for (FuncExpression func : funcList) {
      if (!func.getFuncName().equalsIgnoreCase("first")
          && !func.getFuncName().equalsIgnoreCase("last")) {
        isLastFirst = false;
        break;
      }
    }
    return isLastFirst;
  }

  public List<FuncExpression> getTargetTypeFuncExprList(MappingType mappingType) {
    List<FuncExpression> ret = new ArrayList<>();
    for (Expression expression : expressions) {
      ret.addAll(getTargetTypeFuncExprList(mappingType, expression));
    }
    return ret;
  }

  private List<FuncExpression> getTargetTypeFuncExprList(MappingType mappingType, Expression expr) {
    List<FuncExpression> ret = new ArrayList<>();
    switch (expr.getType()) {
      case Unary:
        UnaryExpression unaryExpr = (UnaryExpression) expr;
        ret.addAll(getTargetTypeFuncExprList(mappingType, unaryExpr.getExpression()));
        break;
      case Bracket:
        BracketExpression bracketExpr = (BracketExpression) expr;
        ret.addAll(getTargetTypeFuncExprList(mappingType, bracketExpr.getExpression()));
        break;
      case Binary:
        BinaryExpression binaryExpr = (BinaryExpression) expr;
        ret.addAll(getTargetTypeFuncExprList(mappingType, binaryExpr.getLeftExpression()));
        ret.addAll(getTargetTypeFuncExprList(mappingType, binaryExpr.getRightExpression()));
        break;
      case Multiple:
        MultipleExpression multipleExpr = (MultipleExpression) expr;
        for (Expression child : multipleExpr.getChildren()) {
          ret.addAll(getTargetTypeFuncExprList(mappingType, child));
        }
        break;
      case Function:
        FuncExpression funcExpr = (FuncExpression) expr;
        String funcName = funcExpr.getFuncName();
        if (FunctionUtils.getFunctionMappingType(funcName) == mappingType) {
          ret.add(funcExpr);
        } else {
          for (Expression expression : funcExpr.getExpressions()) {
            ret.addAll(getTargetTypeFuncExprList(mappingType, expression));
          }
        }
        break;
      case Base:
      case Constant:
      case FromValue:
      case CaseWhen:
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
    }
    return ret;
  }

  public void addExpression(Expression expression) {
    expressions.add(expression);
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }
}
