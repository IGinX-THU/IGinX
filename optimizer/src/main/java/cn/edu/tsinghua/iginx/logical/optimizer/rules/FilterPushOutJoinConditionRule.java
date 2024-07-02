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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import java.util.*;

public class FilterPushOutJoinConditionRule extends Rule {
  private static final Set<OperatorType> validOps =
      new HashSet<>(
          Arrays.asList(
              OperatorType.InnerJoin,
              OperatorType.OuterJoin,
              OperatorType.MarkJoin,
              OperatorType.SingleJoin));

  private static class FilterPushOutJoinConditionRuleInstance {
    private static final FilterPushOutJoinConditionRule INSTANCE =
        new FilterPushOutJoinConditionRule();
  }

  public static FilterPushOutJoinConditionRule getInstance() {
    return FilterPushOutJoinConditionRuleInstance.INSTANCE;
  }

  protected FilterPushOutJoinConditionRule() {
    /*
     * we want to match the topology like:
     *    Inner/Outer/Mark/Single Join
     *       /       \
     *    any        any
     */
    super("FilterPushOutJoinConditionRule", operand(AbstractJoin.class, any(), any()));
  }

  @Override
  public boolean matches(RuleCall call) {
    AbstractJoin join = (AbstractJoin) call.getMatchedRoot();

    if (!validOps.contains(join.getType())) {
      return false;
    }

    List<Filter> splitFilter = LogicalFilterUtils.splitFilter(getJoinFilter(join));

    // 如果有Prefix,根据两侧的prefix进行下推，否则向下找到左右两侧的Project，然后根据Project的列范围进行下推。
    List<Filter> pushFilterA = new ArrayList<>(),
        pushFilterB = new ArrayList<>(),
        remainFilter = new ArrayList<>();

    List<String> leftPatterns =
        OperatorUtils.findPathList(((OperatorSource) join.getSourceA()).getOperator());
    List<String> rightPatterns =
        OperatorUtils.findPathList(((OperatorSource) join.getSourceB()).getOperator());

    for (Filter filter : splitFilter) {
      Filter filterA = LogicalFilterUtils.getSubFilterFromPatterns(filter.copy(), leftPatterns);
      Filter filterB = LogicalFilterUtils.getSubFilterFromPatterns(filter.copy(), rightPatterns);

      if (join.getType() == OperatorType.OuterJoin) {
        // 如果是OuterJoin，那保留表的filter不能下推，例如Left OuterJoin, 左表的filter不能下推，右表可以
        OuterJoin outerJoin = (OuterJoin) join;
        if (outerJoin.getOuterJoinType() == OuterJoinType.LEFT) {
          filterA = new BoolFilter(true);
        } else if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
          filterB = new BoolFilter(true);
        } else {
          filterA = new BoolFilter(true);
          filterB = new BoolFilter(true);
        }
      } else if (join.getType() == OperatorType.MarkJoin
          || join.getType() == OperatorType.SingleJoin) {
        // 如果是MarkJoin或者SingleJoin，左表是保留表，有关的filter不能下推
        filterA = new BoolFilter(true);
      }

      // 如果处理后的filter不是bool类型，那就可以下推
      if (filterA.getType() != FilterType.Bool) {
        pushFilterA.add(filterA);
      }
      if (filterB.getType() != FilterType.Bool) {
        pushFilterB.add(filterB);
      }

      // 如果左右下推的filter都与原filter不同，那就说明原filter中有一部分是不能下推的，那就将其保留
      if (!filter.equals(filterA) && !filter.equals(filterB)) {
        remainFilter.add(filter);
      }
    }

    if (pushFilterA.isEmpty() && pushFilterB.isEmpty()) {
      return false;
    }

    call.setContext(new Object[] {pushFilterA, pushFilterB, remainFilter});

    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    List<Filter> pushFilterA = (List<Filter>) ((Object[]) call.getContext())[0];
    List<Filter> pushFilterB = (List<Filter>) ((Object[]) call.getContext())[1];
    List<Filter> remainFilter = (List<Filter>) ((Object[]) call.getContext())[2];

    AbstractJoin join = (AbstractJoin) call.getMatchedRoot();

    if (!pushFilterA.isEmpty()) {
      join.setSourceA(
          new OperatorSource(
              new Select(join.getSourceA(), new AndFilter(pushFilterA), getJoinTagFilter(join))));
    }
    if (!pushFilterB.isEmpty()) {
      join.setSourceB(
          new OperatorSource(
              new Select(join.getSourceB(), new AndFilter(pushFilterB), getJoinTagFilter(join))));
    }

    if (remainFilter.isEmpty()
        && join.getType() == OperatorType.InnerJoin
        && !((InnerJoin) join).isNaturalJoin()
        && join.getExtraJoinPrefix().isEmpty()) {
      // 如果InnerJoin保留的filter为空，不是NatureJoin,没有ExtraJoinPrefix,将会退化成CrossJoin，
      // 不过一般来说只要ON条件里正常地包含有两侧列的关系，这种情况是不会出现的
      join =
          new CrossJoin(
              join.getSourceA(),
              join.getSourceB(),
              join.getPrefixA(),
              join.getPrefixB(),
              join.getExtraJoinPrefix());
    } else {
      setJoinFilter(
          join, remainFilter.isEmpty() ? new BoolFilter(true) : new AndFilter(remainFilter));
    }
    reChooseJoinAlg(join);
    call.transformTo(join);
  }

  private Filter getJoinFilter(AbstractJoin join) {
    switch (join.getType()) {
      case InnerJoin:
        return ((InnerJoin) join).getFilter();
      case OuterJoin:
        return ((OuterJoin) join).getFilter();
      case MarkJoin:
        return ((MarkJoin) join).getFilter();
      case SingleJoin:
        return ((SingleJoin) join).getFilter();
    }
    throw new IllegalArgumentException("Invalid join type: " + join.getType());
  }

  private void setJoinFilter(AbstractJoin join, Filter filter) {
    switch (join.getType()) {
      case InnerJoin:
        ((InnerJoin) join).setFilter(filter);
        return;
      case OuterJoin:
        ((OuterJoin) join).setFilter(filter);
        return;
      case MarkJoin:
        ((MarkJoin) join).setFilter(filter);
        return;
      case SingleJoin:
        ((SingleJoin) join).setFilter(filter);
        return;
    }
    throw new IllegalArgumentException("Invalid join type: " + join.getType());
  }

  private TagFilter getJoinTagFilter(AbstractJoin join) {
    switch (join.getType()) {
      case InnerJoin:
        return ((InnerJoin) join).getTagFilter();
      case OuterJoin:
        return null;
      case MarkJoin:
        return ((MarkJoin) join).getTagFilter();
      case SingleJoin:
        return ((SingleJoin) join).getTagFilter();
    }
    throw new IllegalArgumentException("Invalid join type: " + join.getType());
  }

  private void reChooseJoinAlg(AbstractJoin join) {
    switch (join.getType()) {
      case InnerJoin:
        ((InnerJoin) join).reChooseJoinAlg();
        return;
      case OuterJoin:
        ((OuterJoin) join).reChooseJoinAlg();
        return;
      case MarkJoin:
        ((MarkJoin) join).reChooseJoinAlg();
        return;
      case SingleJoin:
        ((SingleJoin) join).reChooseJoinAlg();
        return;
    }
  }
}
