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

public class SequenceExpression implements Expression {

  public static String SEQUENCE_PREFIX = "&sequence";

  private final long start;

  private final long increment;

  private final String columnName;

  private String alias;

  public SequenceExpression(final String columnName) {
    this(0, 1, columnName);
  }

  public SequenceExpression(final long start, final long increment, final String columnName) {
    this.start = start;
    this.increment = increment;
    this.columnName = columnName;
  }

  public long getStart() {
    return start;
  }

  public long getIncrement() {
    return increment;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public void setColumnName(String columnName) {}

  @Override
  public ExpressionType getType() {
    return ExpressionType.Sequence;
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
    if (expr == null || expr.getType() != ExpressionType.Sequence) {
      return false;
    }
    SequenceExpression that = (SequenceExpression) expr;
    return this.start == that.start && this.increment == that.increment;
  }
}
