package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.util.*;
import java.util.stream.Collectors;

public class FilterPushDownPathUnionJoinOuterJoinRule extends Rule {
  private static final Set<Class> validOps =
      new HashSet<>(Arrays.asList(PathUnion.class, Join.class, CrossJoin.class));

  private static class InstanceHolder {
    private static final FilterPushDownPathUnionJoinOuterJoinRule instance =
        new FilterPushDownPathUnionJoinOuterJoinRule();
  }

  public FilterPushDownPathUnionJoinOuterJoinRule getInstance() {
    return InstanceHolder.instance;
  }

  protected FilterPushDownPathUnionJoinOuterJoinRule() {
    /*
     * we want to match the topology like:
     *        Select
     *          |
     *     PathUnion/Join
     */
    super(
        "FilterPushDownPathUnionJoinOuterJoinRule",
        operand(Select.class, operand(AbstractBinaryOperator.class)));
  }

  @Override
  public boolean matches(RuleCall call) {
    // PathUnion和Join的下面一般只有其他的PathUnion、Join或者Select，然后就是对Fragment的Project
    // 这里找到左侧和右侧的Fragment对应的列范围，然后将Filter转为CNF,判断是否每个子元素中所有filter的列都在列范围内
    // 如果都在，那就可以下推，否则不能下推
    Select select = (Select) call.getMatchedRoot();
    AbstractBinaryOperator operator =
        (AbstractBinaryOperator) call.getChildrenIndex().get(select).get(0);
    if (!validOps.contains(operator.getClass())) {
      return false;
    }

    List<Project> leftProjects = new ArrayList<>(), rightProjects = new ArrayList<>();
    OperatorUtils.findProjectOperators(leftProjects, call.getChildrenIndex().get(operator).get(0));
    OperatorUtils.findProjectOperators(rightProjects, call.getChildrenIndex().get(operator).get(1));

    List<ColumnsInterval> leftColumnsIntervals = getColumnIntervals(leftProjects);
    List<ColumnsInterval> rightColumnsIntervals = getColumnIntervals(rightProjects);
    String prefixA = null, prefixB = null;

    if (operator.getType() == OperatorType.CrossJoin) {
      prefixA = ((CrossJoin) operator).getPrefixA();
      prefixB = ((CrossJoin) operator).getPrefixB();
    }

    List<Filter> splitFilterList = LogicalFilterUtils.splitFilter(select.getFilter());

    List<Filter> leftPushdownFilters = new ArrayList<>(),
        rightPushdownFilters = new ArrayList<>(),
        remainFilters = new ArrayList<>();

    for (Filter filter : splitFilterList) {
      // 先根据左右的列范围，对filter进行处理，将不再列范围中的部分设置为true,不影响正确性。
      Filter leftFilter, rightFilter;
      if (operator.getType() == OperatorType.OuterJoin) {
        leftFilter = LogicalFilterUtils.getSubFilterFromPrefix(filter.copy(), prefixA);
        rightFilter = LogicalFilterUtils.getSubFilterFromPrefix(filter.copy(), prefixB);
      } else {
        leftFilter =
            LogicalFilterUtils.getSubFilterFromFragments(filter.copy(), leftColumnsIntervals);
        rightFilter =
            LogicalFilterUtils.getSubFilterFromFragments(filter.copy(), rightColumnsIntervals);
      }

      // 然后判断，如果处理完filter是布尔类型,说明无法下推到那一侧，否则可以下推
      if (leftFilter.getType() != FilterType.Bool) {
        leftPushdownFilters.add(leftFilter);
      }
      if (rightFilter.getType() != FilterType.Bool) {
        rightPushdownFilters.add(rightFilter);
      }

      // 然后进行判断，如果左右的filter都跟原filter不一致，说明需要超集下推，原filter要保留在原位置。
      if (!leftFilter.equals(filter) && !rightFilter.equals(filter)) {
        remainFilters.add(filter);
      }
    }

    if (leftPushdownFilters.isEmpty() && rightPushdownFilters.isEmpty()) {
      return false;
    }

    call.setContext(new Object[] {leftPushdownFilters, rightPushdownFilters, remainFilters});

    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    // 获取matches里计算出的结果，然后根据左、右、保留的filter，进行下推
    List<Filter> leftFilters = (List<Filter>) ((Object[]) call.getContext())[0];
    List<Filter> rightFilters = (List<Filter>) ((Object[]) call.getContext())[1];
    List<Filter> remainFilters = (List<Filter>) ((Object[]) call.getContext())[2];

    Select select = (Select) call.getMatchedRoot();
    AbstractBinaryOperator operator =
        (AbstractBinaryOperator) call.getChildrenIndex().get(select).get(0);

    select.setFilter(new AndFilter(remainFilters));
    if (!leftFilters.isEmpty()) {
      Select leftSelect =
          new Select(operator.getSourceA(), new AndFilter(leftFilters), select.getTagFilter());
      operator.setSourceA(new OperatorSource(leftSelect));
    }
    if (!rightFilters.isEmpty()) {
      Select rightSelect =
          new Select(operator.getSourceB(), new AndFilter(rightFilters), select.getTagFilter());
      operator.setSourceB(new OperatorSource(rightSelect));
    }

    // 如果RemainFilters里有PathFilter，可以将CrossJoin转换为InnerJoin
    if (operator.getType() == OperatorType.CrossJoin
        && remainFilters.stream().anyMatch(filter -> filter.getType() == FilterType.Path)) {
      CrossJoin crossJoin = (CrossJoin) operator;
      InnerJoin innerJoin =
          new InnerJoin(
              crossJoin.getSourceA(),
              crossJoin.getSourceB(),
              crossJoin.getPrefixA(),
              crossJoin.getPrefixB(),
              new AndFilter(
                  remainFilters.stream()
                      .filter(filter -> filter.getType() == FilterType.Path)
                      .collect(Collectors.toList())),
              new ArrayList<>());
      innerJoin.reChooseJoinAlg();
      select.setSource(new OperatorSource(innerJoin));
      operator = innerJoin;
    }

    if (!remainFilters.isEmpty()) {
      call.transformTo(select);
    } else {
      call.transformTo(operator);
    }
  }

  private static List<ColumnsInterval> getColumnIntervals(List<Project> projects) {
    return projects.stream()
        .filter(project -> project.getSource() instanceof FragmentSource)
        .map(project -> ((FragmentSource) project.getSource()).getFragment().getColumnsInterval())
        .collect(Collectors.toList());
  }
}
