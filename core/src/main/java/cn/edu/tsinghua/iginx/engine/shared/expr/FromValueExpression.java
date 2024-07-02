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
package cn.edu.tsinghua.iginx.engine.shared.expr;

import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;

public class FromValueExpression implements Expression {

  private final SelectStatement subStatement;

  public FromValueExpression(SelectStatement subStatement) {
    this.subStatement = subStatement;
  }

  public SelectStatement getSubStatement() {
    return subStatement;
  }

  @Override
  public String getColumnName() {
    return "";
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.FromValue;
  }

  @Override
  public boolean hasAlias() {
    return false;
  }

  @Override
  public String getAlias() {
    return null;
  }

  @Override
  public void setAlias(String alias) {}

  @Override
  public void accept(ExpressionVisitor visitor) {
    visitor.visit(this);
  }
}
