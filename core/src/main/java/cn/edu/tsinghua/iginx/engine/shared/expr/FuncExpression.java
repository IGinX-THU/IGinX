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

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FuncExpression implements Expression {

  private final String funcName;
  private final FunctionParams params;
  private final boolean isDistinct;
  private final boolean isPyUDF;
  private String alias;

  public static final int COLUMN_ARG_TYPE = 0;
  public static final int POS_ARG_TYPE = 1;

  public FuncExpression(String funcName, List<String> columns) {
    this(funcName, getArgListFromColumns(columns), new HashMap<>(), "", false);
  }

  public FuncExpression(
      String funcName,
      List<Pair<Integer, Object>> args,
      Map<String, Object> kvargs,
      boolean isDistinct) {
    this(funcName, args, kvargs, "", isDistinct);
  }

  public FuncExpression(
      String funcName,
      List<Pair<Integer, Object>> args,
      Map<String, Object> kvargs,
      String alias,
      boolean isDistinct) {
    this.funcName = funcName;
    this.params = new FunctionParams(args, kvargs);
    this.alias = alias;
    this.isDistinct = isDistinct;
    this.isPyUDF = FunctionUtils.isPyUDF(funcName);
  }

  public static List<Pair<Integer, Object>> getArgListFromColumns(List<String> columns) {
    List<Pair<Integer, Object>> list = new ArrayList<>();
    for (String column : columns) {
      list.add(new Pair<>(COLUMN_ARG_TYPE, column));
    }
    return list;
  }

  public String getFuncName() {
    return funcName;
  }

  /**
   * 获取位置参数中所有列名
   *
   * @return 一个参数列表
   */
  public List<String> getColumns() {
    return params.getPaths();
  }

  /**
   * 获取位置参数中所有常量参数
   *
   * @return 一个参数列表
   */
  public List<Object> getConPosArgs() {
    return params.getConPosArgs();
  }

  /**
   * 获取所有kv参数
   *
   * @return 一个参数列表
   */
  public Map<String, Object> getKvargs() {
    return params.getKwargs();
  }

  /**
   * 获取所有位置参数，包括列名和常量
   *
   * @return 一个参数列表，Pair.k指示参数类型（列名或常量），Pair.v指示值（列名或常量的值）
   */
  public List<Pair<Integer, Object>> getPosArgs() {
    return params.getPosArgs();
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
    return columnName + String.join(", ", getColumns()) + ")";
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
