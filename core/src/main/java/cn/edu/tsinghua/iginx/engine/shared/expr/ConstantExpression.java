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

import java.util.Arrays;

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
    } else if (value instanceof byte[]) {
      return "'" + new String((byte[]) value) + "'";
    }
    return value.toString();
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Constant;
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
    if (expr == null || expr.getType() != ExpressionType.Constant) {
      return false;
    }
    ConstantExpression that = (ConstantExpression) expr;
    if (value instanceof byte[]) {
      return Arrays.equals((byte[]) value, (byte[]) that.value);
    } else {
      return this.value.equals(that.value);
    }
  }
}
