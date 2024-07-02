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

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FuncExpression implements Expression {

  private final String funcName;
  private final List<String> columns;
  private final List<Object> args;
  private final Map<String, Object> kvargs;
  private final boolean isDistinct;
  private final boolean isPyUDF;
  private String alias;

  public FuncExpression(String funcName, List<String> columns) {
    this(funcName, columns, new ArrayList<>(), new HashMap<>(), "", false);
  }

  public FuncExpression(
      String funcName,
      List<String> columns,
      List<Object> args,
      Map<String, Object> kvargs,
      boolean isDistinct) {
    this(funcName, columns, args, kvargs, "", isDistinct);
  }

  public FuncExpression(
      String funcName,
      List<String> columns,
      List<Object> args,
      Map<String, Object> kvargs,
      String alias,
      boolean isDistinct) {
    this.funcName = funcName;
    this.columns = columns;
    this.args = args;
    this.kvargs = kvargs;
    this.alias = alias;
    this.isDistinct = isDistinct;
    this.isPyUDF = FunctionUtils.isPyUDF(funcName);
  }

  public String getFuncName() {
    return funcName;
  }

  public List<String> getColumns() {
    return columns;
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
    String columnName = isPyUDF ? funcName : funcName.toLowerCase();
    columnName += "(";
    if (isDistinct) {
      columnName += "distinct ";
    }
    return columnName + String.join(", ", columns) + ")";
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.Function;
  }

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
}
