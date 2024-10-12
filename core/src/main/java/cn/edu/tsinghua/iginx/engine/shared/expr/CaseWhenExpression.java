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
}
