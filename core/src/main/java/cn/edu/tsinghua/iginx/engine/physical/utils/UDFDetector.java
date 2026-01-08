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
package cn.edu.tsinghua.iginx.engine.physical.utils;

import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

public class UDFDetector {

  public static boolean containNonSystemFunction(@Nullable FunctionCall functionCall) {
    if (functionCall == null) {
      return false;
    }
    if (containNonSystemFunction(functionCall.getFunction())) {
      return true;
    }
    FunctionParams params = functionCall.getParams();
    for (Expression expression : params.getExpressions()) {
      if (containNonSystemFunction(expression)) {
        return true;
      }
    }
    return false;
  }

  public static boolean containNonSystemFunction(@Nullable Function function) {
    if (function == null) {
      return false;
    }
    return function.getFunctionType() != FunctionType.System;
  }

  public static boolean containNonSystemFunction(@Nullable Filter filter) {
    if (filter == null) {
      return false;
    }
    boolean[] result = new boolean[1];
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {}

          @Override
          public void visit(PathFilter filter) {}

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            if (containNonSystemFunction(filter.getExpressionA())
                || containNonSystemFunction(filter.getExpressionB())) {
              result[0] = true;
            }
          }

          @Override
          public void visit(InFilter filter) {}
        });
    return result[0];
  }

  public static boolean containNonSystemFunction(@Nullable Expression expression) {
    if (expression == null) {
      return false;
    }
    AtomicBoolean containUdf = new AtomicBoolean(false);
    expression.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {}

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {
            if (containNonSystemFunction(
                FunctionManager.getInstance().getFunction(expression.getFuncName()))) {
              containUdf.set(true);
            }
          }

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}

          @Override
          public void visit(CaseWhenExpression expression) {}

          @Override
          public void visit(KeyExpression expression) {}

          @Override
          public void visit(SequenceExpression expression) {}
        });
    return containUdf.get();
  }
}
