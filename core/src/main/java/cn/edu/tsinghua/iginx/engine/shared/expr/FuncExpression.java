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
package cn.edu.tsinghua.iginx.engine.shared.expr;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FuncExpression implements Expression {

  private final String funcName;
  private final List<Expression> expressions;
  private final List<Object> args;
  private final Map<String, Object> kvargs;
  private final boolean isDistinct;
  private final boolean isPyUDF;
  private String alias;

  public FuncExpression(String funcName, List<Expression> expressions) {
    this(funcName, expressions, new ArrayList<>(), new HashMap<>(), "", false);
  }

  public FuncExpression(
      String funcName,
      List<Expression> expressions,
      List<Object> args,
      Map<String, Object> kvargs,
      boolean isDistinct) {
    this(funcName, expressions, args, kvargs, "", isDistinct);
  }

  public FuncExpression(
      String funcName,
      List<Expression> expressions,
      List<Object> args,
      Map<String, Object> kvargs,
      String alias,
      boolean isDistinct) {
    this.funcName = funcName;
    this.expressions = expressions;
    this.args = args;
    this.kvargs = kvargs;
    this.alias = alias;
    this.isDistinct = isDistinct;
    this.isPyUDF = FunctionUtils.isPyUDF(funcName);
  }

  public String getFuncName() {
    return funcName;
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public List<Object> getArgs() {
    return args;
  }

  public Map<String, Object> getKvargs() {
    return kvargs;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public boolean isPyUDF() {
    return isPyUDF;
  }

  @Override
  public String getColumnName() {
    StringBuilder columnName = new StringBuilder(isPyUDF ? funcName : funcName.toLowerCase());
    columnName.append("(");
    if (isDistinct) {
      columnName.append("distinct ");
    }
    for (Expression expression : expressions) {
      columnName.append(expression.getColumnName()).append(", ");
    }
    for (Object arg : args) {
      columnName.append(ValueUtils.toString(arg)).append(", ");
    }
    for (Map.Entry<String, Object> kvarg : kvargs.entrySet()) {
      columnName.append(kvarg.getValue()).append(", ");
    }
    columnName.setLength(columnName.length() - 2);
    columnName.append(")");
    return columnName.toString();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Function;
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.isEmpty();
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(this);
    expressions.forEach(e -> e.accept(visitor));
  }

  @Override
  public boolean equalExceptAlias(Expression expr) {
    if (this == expr) {
      return true;
    }
    if (expr == null || expr.getType() != ExpressionType.Function) {
      return false;
    }
    FuncExpression that = (FuncExpression) expr;

    if (this.isPyUDF != that.isPyUDF) {
      return false;
    }
    if (this.isPyUDF) {
      if (!this.funcName.equals(that.funcName)) {
        return false;
      }
    } else {
      if (!this.funcName.equalsIgnoreCase(that.funcName)) {
        return false;
      }
    }

    if (this.getExpressions().size() != that.getExpressions().size()) {
      return false;
    }
    for (int i = 0; i < this.getExpressions().size(); i++) {
      if (!this.getExpressions().get(i).equalExceptAlias(that.getExpressions().get(i))) {
        return false;
      }
    }

    return this.args.equals(that.args)
        && this.kvargs.equals(that.kvargs)
        && this.isDistinct == that.isDistinct;
  }
}
