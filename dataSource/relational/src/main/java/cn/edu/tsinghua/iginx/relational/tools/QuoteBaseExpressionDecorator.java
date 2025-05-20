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
package cn.edu.tsinghua.iginx.relational.tools;

import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.ExpressionVisitor;

public class QuoteBaseExpressionDecorator implements Expression {
  private final BaseExpression baseExpression;
  private final char quote;

  private static String DERIVED = "derived";

  public QuoteBaseExpressionDecorator(BaseExpression baseExpression, char quote) {
    this.baseExpression = baseExpression;
    this.quote = quote;
  }

  @Override
  public String getColumnName() {
    return DERIVED + "." + quote + baseExpression.getColumnName() + quote;
  }

  @Override
  public ExpressionType getType() {
    return baseExpression.getType();
  }

  @Override
  public boolean hasAlias() {
    return baseExpression.hasAlias();
  }

  @Override
  public String getAlias() {
    return baseExpression.getAlias();
  }

  @Override
  public void setAlias(String alias) {
    baseExpression.setAlias(alias);
  }

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(baseExpression);
  }

  @Override
  public boolean equalExceptAlias(Expression expr) {
    return baseExpression.equalExceptAlias(expr);
  }
}
