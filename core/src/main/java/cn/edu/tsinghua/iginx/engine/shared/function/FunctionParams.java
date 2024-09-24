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

package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionParams {

  private final List<Pair<Integer, Object>> posArgs;

  private final Map<String, Object> kwargs;

  private Expression expr;

  private boolean isDistinct;

  public FunctionParams(List<String> paths) {
    this(FuncExpression.getArgListFromColumns(paths), null, null, false);
  }

  public FunctionParams(Expression expr) {
    this(null, null, expr, false);
  }

  public FunctionParams(List<Pair<Integer, Object>> posArgs, Map<String, Object> kwargs) {
    this(posArgs, kwargs, null, false);
  }

  public FunctionParams(
      List<Pair<Integer, Object>> posArgs, Map<String, Object> kwargs, boolean isDistinct) {
    this(posArgs, kwargs, null, isDistinct);
  }

  public FunctionParams(
      List<Pair<Integer, Object>> posArgs,
      Map<String, Object> kwargs,
      Expression expr,
      boolean isDistinct) {
    this.posArgs = posArgs;
    this.kwargs = kwargs;
    this.expr = expr;
    this.isDistinct = isDistinct;
  }

  public List<String> getPaths() {
    List<String> list = new ArrayList<>();
    for (Pair<Integer, Object> p : posArgs) {
      if (p.k == FuncExpression.COLUMN_ARG_TYPE) {
        list.add(p.v.toString());
      }
    }
    return list;
  }

  /**
   * 获取位置参数中所有常量参数
   *
   * @return 一个参数列表
   */
  public List<Object> getConPosArgs() {
    List<Object> list = new ArrayList<>();
    for (Pair<Integer, Object> p : posArgs) {
      if (p.k == FuncExpression.POS_ARG_TYPE) {
        list.add(p.v);
      }
    }
    return list;
  }

  /**
   * 获取所有位置参数，包括列名和常量
   *
   * @return 一个参数列表，Pair.k指示参数类型（列名或常量），Pair.v指示值（列名或常量的值）
   */
  public List<Pair<Integer, Object>> getPosArgs() {
    return posArgs;
  }

  public Map<String, Object> getKwargs() {
    return kwargs;
  }

  public Expression getExpr() {
    return expr;
  }

  public void setExpr(Expression expr) {
    this.expr = expr;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean distinct) {
    isDistinct = distinct;
  }

  protected FunctionParams copy() {
    List<Pair<Integer, Object>> newPosArgs = null;
    if (posArgs != null && !posArgs.isEmpty()) {
      newPosArgs = new ArrayList<>(posArgs);
    }
    Map<String, Object> newKvargs = null;
    if (kwargs != null && !kwargs.isEmpty()) {
      newKvargs = new HashMap<>(kwargs);
    }
    return new FunctionParams(newPosArgs, newKvargs, expr, isDistinct);
  }
}
