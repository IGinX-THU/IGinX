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

public class KeyExpression implements Expression {

  public static String KEY_PREFIX = "&key@";

  private final String columnName;

  private String alias;

  public KeyExpression(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Key;
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
  }

  @Override
  public boolean equalExceptAlias(Expression expr) {
    if (this == expr) {
      return true;
    }
    return expr != null && expr.getType() == ExpressionType.Key;
  }
}
