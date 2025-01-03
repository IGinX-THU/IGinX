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
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class FilterPushDownAddSchemaPrefixRule extends Rule {

  public FilterPushDownAddSchemaPrefixRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *     AddSchemaPrefix
     */
    super(
        "FilterPushDownAddSchemaPrefixRule",
        "FilterPushDownRule",
        operand(Select.class, operand(AddSchemaPrefix.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AddSchemaPrefix addSchemaPrefix =
        (AddSchemaPrefix) ((OperatorSource) select.getSource()).getOperator();
    removeFilterPrefix(select.getFilter(), addSchemaPrefix.getSchemaPrefix());

    select.setSource(addSchemaPrefix.getSource());
    addSchemaPrefix.setSource(new OperatorSource(select));
    call.transformTo(addSchemaPrefix);
  }

  private void removeFilterPrefix(Filter filter, String prefix) {
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
          public void visit(ValueFilter filter) {
            filter.setPath(removePrefix(filter.getPath(), prefix));
          }

          @Override
          public void visit(PathFilter filter) {
            filter.setPathA(removePrefix(filter.getPathA(), prefix));
            filter.setPathB(removePrefix(filter.getPathB(), prefix));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            Expression exprA = filter.getExpressionA();
            Expression exprB = filter.getExpressionB();
            removeExpressionPrefix(exprA, prefix);
            removeExpressionPrefix(exprB, prefix);
          }

          @Override
          public void visit(InFilter inFilter) {
            inFilter.setPath(removePrefix(inFilter.getPath(), prefix));
          }
        });
  }

  private void removeExpressionPrefix(Expression expression, String prefix) {
    expression.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            expression.setPathName(removePrefix(expression.getPathName(), prefix));
          }

          @Override
          public void visit(BinaryExpression expression) {}

          @Override
          public void visit(BracketExpression expression) {}

          @Override
          public void visit(ConstantExpression expression) {}

          @Override
          public void visit(FromValueExpression expression) {}

          @Override
          public void visit(FuncExpression expression) {}

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
  }

  private String removePrefix(String path, String prefix) {
    if (prefix != null && path.startsWith(prefix + ".")) {
      return path.substring(prefix.length() + 1);
    }
    return path;
  }
}
