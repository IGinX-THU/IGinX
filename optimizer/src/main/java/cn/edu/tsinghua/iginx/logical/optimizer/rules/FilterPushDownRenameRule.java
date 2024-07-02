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
package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.Map;

public class FilterPushDownRenameRule extends Rule {
  private static class InstanceHolder {
    private static final FilterPushDownRenameRule INSTANCE = new FilterPushDownRenameRule();
  }

  public static FilterPushDownRenameRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterPushDownRenameRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *         Rename
     */
    super("FilterPushDownRenameRule", operand(Select.class, operand(Rename.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    return super.matches(call);
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    Rename rename = (Rename) ((OperatorSource) select.getSource()).getOperator();
    select.setFilter(replacePathByRenameMap(select.getFilter(), rename.getAliasMap()));
    select.setSource(rename.getSource());
    rename.setSource(new OperatorSource(select));
    call.transformTo(rename);
  }

  private Filter replacePathByRenameMap(Filter filter, Map<String, String> renameMap) {
    Filter newFilter = filter.copy();
    newFilter.accept(
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
            filter.setPath(PathUtils.recoverRenamedPattern(renameMap, filter.getPath()));
          }

          @Override
          public void visit(PathFilter filter) {
            filter.setPathA(PathUtils.recoverRenamedPattern(renameMap, filter.getPathA()));
            filter.setPathB(PathUtils.recoverRenamedPattern(renameMap, filter.getPathB()));
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            replaceExpressionByRenameMap(filter.getExpressionA(), renameMap);
            replaceExpressionByRenameMap(filter.getExpressionB(), renameMap);
          }
        });

    return newFilter;
  }

  private void replaceExpressionByRenameMap(Expression expression, Map<String, String> renameMap) {
    expression.accept(
        new ExpressionVisitor() {
          @Override
          public void visit(BaseExpression expression) {
            expression.setPathName(
                PathUtils.recoverRenamedPattern(renameMap, expression.getPathName()));
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
          public void visit(FuncExpression expression) {
            expression
                .getColumns()
                .replaceAll(column -> PathUtils.recoverRenamedPattern(renameMap, column));
          }

          @Override
          public void visit(MultipleExpression expression) {}

          @Override
          public void visit(UnaryExpression expression) {}
        });
  }
}
