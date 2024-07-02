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

import cn.edu.tsinghua.iginx.engine.shared.operator.AbstractJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;

public class FilterJoinTransposeRule extends Rule {

  private static final class InstanceHolder {
    static final FilterJoinTransposeRule INSTANCE = new FilterJoinTransposeRule();
  }

  public static FilterJoinTransposeRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected FilterJoinTransposeRule() {
    /*
     * we want to match the topology like:
     *         Select
     *           |
     *      AbstractJoin
     *        /     \
     *     Any       Any
     */
    super(
        "FilterJoinTransposeRule",
        operand(Select.class, operand(AbstractJoin.class, any(), any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);
    // we only deal with cross/inner/outer join for now.
    return !(join instanceof MarkJoin) && !(join instanceof SingleJoin);
  }

  @Override
  public void onMatch(RuleCall call) {
    Select select = (Select) call.getMatchedRoot();
    AbstractJoin join = (AbstractJoin) call.getChildrenIndex().get(select).get(0);

    Source sourceA = join.getSourceA();
    Source sourceB = join.getSourceB();

    Select selectA = (Select) select.copyWithSource(sourceA);
    Select selectB = (Select) select.copyWithSource(sourceB);
    AbstractJoin newJoin =
        (AbstractJoin)
            join.copyWithSource(new OperatorSource(selectA), new OperatorSource(selectB));
    call.transformTo(newJoin);
  }
}
