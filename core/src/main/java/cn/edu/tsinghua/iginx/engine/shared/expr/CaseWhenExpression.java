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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ExprFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import java.util.List;

public class CaseWhenExpression implements Expression {

  public static String CASE_WHEN_PREFIX = "&case";
  private final List<Filter> conditions;
  private final List<Expression> results;
  private Expression resultElse;
  private final String columnName;
  private String alias;

  public CaseWhenExpression(
      List<Filter> conditions, List<Expression> results, Expression resultElse, String columnName) {
    this(conditions, results, resultElse, columnName, "");
  }

  public CaseWhenExpression(
      List<Filter> conditions,
      List<Expression> results,
      Expression resultElse,
      String columnName,
      String alias) {
    this.conditions = conditions;
    this.results = results;
    this.resultElse = resultElse;
    this.columnName = columnName;
    this.alias = alias;
  }

  public List<Filter> getConditions() {
    return conditions;
  }

  public List<Expression> getResults() {
    return results;
  }

  public Expression getResultElse() {
    return resultElse;
  }

  public void setResultElse(Expression resultElse) {
    this.resultElse = resultElse;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public void setColumnName(String columnName) {}

  @Override
  public ExpressionType getType() {
    return ExpressionType.CaseWhen;
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
    for (Filter condition : conditions) {
      if (condition.getType().equals(FilterType.Expr)) {
        ExprFilter exprFilter = (ExprFilter) condition;
        exprFilter.getExpressionA().accept(visitor);
        exprFilter.getExpressionB().accept(visitor);
      }
    }
    for (Expression result : results) {
      result.accept(visitor);
    }
    resultElse.accept(visitor);
  }

  @Override
  public boolean equalExceptAlias(Expression expr) {
    if (this == expr) {
      return true;
    }
    if (expr == null || expr.getType() != ExpressionType.CaseWhen) {
      return false;
    }
    CaseWhenExpression that = (CaseWhenExpression) expr;
    if (this.conditions.size() != that.conditions.size()) {
      return false;
    }
    for (int i = 0; i < this.conditions.size(); i++) {
      if (!this.conditions.get(i).equals(that.conditions.get(i))) {
        return false;
      }
    }
    if (this.results.size() != that.results.size()) {
      return false;
    }
    for (int i = 0; i < this.results.size(); i++) {
      if (!this.results.get(i).equalExceptAlias(that.results.get(i))) {
        return false;
      }
    }
    if (this.resultElse == null && that.resultElse == null) {
      return true;
    }
    if (this.resultElse == null || that.resultElse == null) {
      return false;
    }
    return this.resultElse.equalExceptAlias(that.resultElse);
  }
}
