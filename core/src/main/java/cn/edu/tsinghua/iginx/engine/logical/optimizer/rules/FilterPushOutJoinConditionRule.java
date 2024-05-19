package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
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
import java.util.*;

public class FilterPushOutJoinConditionRule extends Rule {
  private static final Set<OperatorType> validOps =
      new HashSet<>(Arrays.asList(OperatorType.InnerJoin, OperatorType.OuterJoin));

  private static class FilterPushOutJoinConditionRuleInstance {
    private static final FilterPushOutJoinConditionRule INSTANCE =
        new FilterPushOutJoinConditionRule();
  }

  public FilterPushOutJoinConditionRule getInstance() {
    return FilterPushOutJoinConditionRuleInstance.INSTANCE;
  }

  protected FilterPushOutJoinConditionRule() {
    /*
     * we want to match the topology like:
     *    Inner/Outer Join
     *       /       \
     *    any        any
     */
    super(
        "FilterPushOutJoinCondition",
        operand(Select.class, operand(AbstractJoin.class, any(), any())));
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
    String prefixA = join.getPrefixA(), prefixB = join.getPrefixB();
    List<String> leftPatterns = new ArrayList<>(), rightPatterns = new ArrayList<>();
    if (prefixA == null || prefixB == null) {
      leftPatterns = OperatorUtils.findPathList(((OperatorSource) join.getSourceA()).getOperator());
      rightPatterns =
          OperatorUtils.findPathList(((OperatorSource) join.getSourceB()).getOperator());
    }

    for (Filter filter : splitFilter) {
      Filter filterA = getSubFilter(filter.copy(), prefixA, leftPatterns);
      Filter filterB = getSubFilter(filter.copy(), prefixB, rightPatterns);

      // 如果是OuterJoin，那保留表的filter不能下推，例如Left OuterJoin, 左表的filter不能下推，右表可以
      if (join.getType() == OperatorType.OuterJoin) {
        OuterJoin outerJoin = (OuterJoin) join;
        if (outerJoin.getOuterJoinType() == OuterJoinType.LEFT) {
          filterA = new BoolFilter(true);
        } else if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
          filterB = new BoolFilter(true);
        } else {
          filterA = new BoolFilter(true);
          filterB = new BoolFilter(true);
        }
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

    if (remainFilter.isEmpty()) {
      // 如果保留的filter为空，将会退化成CrossJoin，不过一般来说只要ON条件里正常地包含有两侧列的关系，这种情况是不会出现的
      call.transformTo(
          new CrossJoin(
              join.getSourceA(), join.getSourceB(), join.getPrefixA(), join.getPrefixB()));
    } else {
      setJoinFilter(join, new AndFilter(remainFilter));
      call.transformTo(join);
    }
  }

  private Filter getJoinFilter(AbstractJoin join) {
    if (join.getType() == OperatorType.InnerJoin) {
      return ((InnerJoin) join).getFilter();
    } else if (join.getType() == OperatorType.OuterJoin) {
      return ((OuterJoin) join).getFilter();
    }
    throw new IllegalArgumentException("Invalid join type: " + join.getType());
  }

  private void setJoinFilter(AbstractJoin join, Filter filter) {
    if (join.getType() == OperatorType.InnerJoin) {
      ((InnerJoin) join).setFilter(filter);
    } else if (join.getType() == OperatorType.OuterJoin) {
      ((OuterJoin) join).setFilter(filter);
    }
    throw new IllegalArgumentException("Invalid join type: " + join.getType());
  }

  private TagFilter getJoinTagFilter(AbstractJoin join) {
    if (join.getType() == OperatorType.InnerJoin) {
      return ((InnerJoin) join).getTagFilter();
    } else if (join.getType() == OperatorType.OuterJoin) {
      return null;
    }
    throw new IllegalArgumentException("Invalid join type: " + join.getType());
  }

  private Filter getSubFilter(Filter filter, String prefix, List<String> patterns) {
    if (prefix != null) {
      return LogicalFilterUtils.getSubFilterFromPrefix(filter, prefix);
    }
    return LogicalFilterUtils.getSubFilterFromPatterns(filter, patterns);
  }
}
