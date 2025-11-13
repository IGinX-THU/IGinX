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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FunctionParams {

  private List<String> paths;

  private final List<Expression> expressions;

  private final List<Object> args;

  private final Map<String, Object> kwargs;

  private boolean isDistinct;

  public FunctionParams(Expression expression) {
    this(Collections.singletonList(expression), null, null, false);
  }

  public FunctionParams(List<Expression> expressions) {
    this(expressions, null, null, false);
  }

  public FunctionParams(
      List<Expression> expressions, List<Object> args, Map<String, Object> kwargs) {
    this(expressions, args, kwargs, false);
  }

  public FunctionParams(
      List<Expression> expressions,
      List<Object> args,
      Map<String, Object> kwargs,
      boolean isDistinct) {
    this.paths = expressions.stream().map(Expression::getColumnName).collect(Collectors.toList());
    this.expressions = expressions;
    this.args = args;
    this.kwargs = kwargs;
    this.isDistinct = isDistinct;
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public Expression getExpression(int i) {
    return expressions.get(i);
  }

  public void setExpression(int i, Expression expression) {
    expressions.set(i, expression);
    paths.set(i, expression.getColumnName());
  }

  public List<String> getPaths() {
    return paths;
  }

  public void updatePaths() {
    this.paths = expressions.stream().map(Expression::getColumnName).collect(Collectors.toList());
  }

  public List<Object> getArgs() {
    return args;
  }

  public Map<String, Object> getKwargs() {
    return kwargs;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean distinct) {
    isDistinct = distinct;
  }

  protected FunctionParams copy() {
    List<Expression> newExpressions = new ArrayList<>(expressions.size());
    for (Expression expression : expressions) {
      newExpressions.add(ExprUtils.copy(expression));
    }
    Map<String, Object> newKwargs = null;
    if (kwargs != null) {
      newKwargs = new HashMap<>(kwargs);
    }
    List<Object> newArgs = null;
    if (args != null) {
      newArgs = new ArrayList<>(args);
    }
    return new FunctionParams(newExpressions, newArgs, newKwargs, isDistinct);
  }
}
