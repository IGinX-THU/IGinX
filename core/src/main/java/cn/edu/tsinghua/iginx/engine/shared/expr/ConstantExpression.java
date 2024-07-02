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

public class ConstantExpression implements Expression {

  private final Object value;
  private String alias;

  public ConstantExpression(Object value) {
    this(value, "");
  }

  public ConstantExpression(Object value, String alias) {
    this.value = value;
    this.alias = alias;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String getColumnName() {
    // 如果是小数，保留小数点后5位
    if (value instanceof Double || value instanceof Float) {
      return String.format("%.5f", value);
    }
    return value.toString();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Constant;
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.equals("");
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
}
