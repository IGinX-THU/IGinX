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

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import com.google.auto.service.AutoService;

@AutoService(Rule.class)
public class SingleJoinEliminateRule extends Rule {

  public SingleJoinEliminateRule() {
    /*
     * we want to match the topology like:
     *      Single Join
     *       /       \
     *    any        AbstractUnaryOperator
     */
    super(
        "SingleJoinEliminateRule",
        "FilterPushDownRule",
        operand(SingleJoin.class, any(), operand(AbstractUnaryOperator.class, any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    SingleJoin join = (SingleJoin) call.getMatchedRoot();

    AbstractUnaryOperator expectSingleRow =
        (AbstractUnaryOperator) ((OperatorSource) join.getSourceB()).getOperator();
    return isSingleRowOperator(expectSingleRow);
  }

  @Override
  public void onMatch(RuleCall call) {
    SingleJoin singleJoin = (SingleJoin) call.getMatchedRoot();

    CrossJoin newJoin =
        new CrossJoin(
            singleJoin.getSourceA(),
            singleJoin.getSourceB(),
            singleJoin.getPrefixA(),
            singleJoin.getPrefixB(),
            singleJoin.getExtraJoinPrefix());

    Select newRoot =
        new Select(new OperatorSource(newJoin), singleJoin.getFilter(), singleJoin.getTagFilter());

    call.transformTo(newRoot);
  }

  private static boolean isSingleRowOperator(AbstractUnaryOperator operator) {
    switch (operator.getType()) {
      case SetTransform:
        return true;
      case Project:
      case Select:
      case Sort:
      case Limit:
      case Downsample:
      case RowTransform:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:;
      case GroupBy:
      case Distinct:
      case AddSequence:
      case RemoveNullColumn:
        if (operator.getSource().getType() != SourceType.Operator) {
          return false;
        }
        OperatorSource source = (OperatorSource) operator.getSource();
        if (!(source.getOperator() instanceof AbstractUnaryOperator)) {
          return false;
        }
        return isSingleRowOperator((AbstractUnaryOperator) source.getOperator());
      default:
        return false;
    }
  }
}
