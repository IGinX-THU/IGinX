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
package cn.edu.tsinghua.iginx.engine.shared.function;

import static cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr.ARITHMETIC_EXPR;

import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDTF;

public class FunctionCall {

  private final Function function;

  private final FunctionParams params;

  private final boolean needPreRowTransform;

  public FunctionCall(Function function, FunctionParams params) {
    this.function = function;
    this.params = params;

    // init needPreRowTransform
    boolean needPreRowTransform = false;
    if (!function.getIdentifier().equalsIgnoreCase(ARITHMETIC_EXPR)) {
      for (Expression expression : params.getExpressions()) {
        if (!(expression instanceof BaseExpression)) {
          needPreRowTransform = true;
          break;
        }
      }
    }
    this.needPreRowTransform = needPreRowTransform;
  }

  public Function getFunction() {
    return function;
  }

  public FunctionParams getParams() {
    return params;
  }

  public boolean isNeedPreRowTransform() {
    return needPreRowTransform;
  }

  public FunctionCall copy() {
    return new FunctionCall(function, params.copy());
  }

  @Override
  public String toString() {
    return String.format(
        "{Name: %s, FuncType: %s, MappingType: %s}",
        function.getIdentifier(), function.getFunctionType(), function.getMappingType());
  }

  private String getFuncName() {
    if (function instanceof ArithmeticExpr) {
      if (params.getExpressions().size() == 1) {
        return params.getExpressions().get(0).getColumnName();
      }
    } else if (function.getFunctionType() == FunctionType.UDF) {
      if (function instanceof UDAF) {
        return ((UDAF) function).getFunctionName();
      } else if (function instanceof UDTF) {
        return ((UDTF) function).getFunctionName();
      } else if (function instanceof UDSF) {
        return ((UDSF) function).getFunctionName();
      }
    } else {
      return function.getIdentifier();
    }
    return null;
  }

  public String getFunctionStr() {
    if (function instanceof ArithmeticExpr) {
      return params.getExpressions().get(0).getColumnName();
    }

    return String.format(
        "%s(%s%s)",
        getFuncName(),
        params.isDistinct() ? "distinct " : "",
        String.join(", ", params.getPaths()));
  }

  public String getNameAndFuncTypeStr() {
    return String.format(
        "(%s, %s)", FunctionUtils.getFunctionName(function), function.getFunctionType());
  }
}
