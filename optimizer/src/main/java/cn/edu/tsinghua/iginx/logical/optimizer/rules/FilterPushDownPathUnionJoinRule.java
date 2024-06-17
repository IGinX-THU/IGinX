package cn.edu.tsinghua.iginx.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.util.*;
import java.util.stream.Collectors;

public class FilterPushDownPathUnionJoinRule extends Rule {
  private static final Set<OperatorType> validOps =
      new HashSet<>(
          Arrays.asList(
              OperatorType.Join,
              OperatorType.PathUnion,
              OperatorType.OuterJoin,
              OperatorType.MarkJoin,
              OperatorType.SingleJoin));

  private static final Set<OperatorType> needRemain =
      new HashSet<>(Arrays.asList(OperatorType.Join, OperatorType.PathUnion));

  private static final Map<Select, String> selectMap = new HashMap<>();

  private static class InstanceHolder {
    private static final FilterPushDownPathUnionJoinRule instance =
        new FilterPushDownPathUnionJoinRule();
  }

  public static FilterPushDownPathUnionJoinRule getInstance() {
    return InstanceHolder.instance;
  }

  protected FilterPushDownPathUnionJoinRule() {
    /*
     * we want to match the topology like:
     *        Select
     *          |
     *     PathUnion/Join
     */
    super(
        "FilterPushDownPathUnionJoinRule",
        operand(Select.class, operand(AbstractBinaryOperator.class, any(), any())));
  }

  @Override
  public boolean matches(RuleCall call) {
    // PathUnion和Join的下面一般只有其他的PathUnion、Join或者Select，然后就是对Fragment的Project
    // 这里找到左侧和右侧的Fragment对应的列范围，然后将Filter转为CNF,判断是否每个子元素中所有filter的列都在列范围内
    // 如果都在，那就可以下推，否则不能下推
    Select select = (Select) call.getMatchedRoot();
    AbstractBinaryOperator operator =
        (AbstractBinaryOperator) call.getChildrenIndex().get(select).get(0);
    if (!validOps.contains(operator.getType())
        || selectMap.containsKey(select)
            && selectMap.get(select).equals(select.getFilter().toString())) {
      return false;
    }

    List<Project> leftProjects = new ArrayList<>(), rightProjects = new ArrayList<>();
    OperatorUtils.findProjectOperators(leftProjects, call.getChildrenIndex().get(operator).get(0));
    OperatorUtils.findProjectOperators(rightProjects, call.getChildrenIndex().get(operator).get(1));

    List<ColumnsInterval> leftColumnsIntervals = getColumnIntervals(leftProjects);
    List<ColumnsInterval> rightColumnsIntervals = getColumnIntervals(rightProjects);
    String prefixA = null, prefixB = null;
    List<String> leftPatterns = new ArrayList<>(), rightPatterns = new ArrayList<>();

    if (operator.getType() == OperatorType.OuterJoin) {
      prefixA = ((OuterJoin) operator).getPrefixA();
      prefixB = ((OuterJoin) operator).getPrefixB();
    } else if (operator.getType() == OperatorType.MarkJoin
        || operator.getType() == OperatorType.SingleJoin) {
      leftPatterns =
          OperatorUtils.findPathList(((OperatorSource) operator.getSourceA()).getOperator());
      rightPatterns =
          OperatorUtils.findPathList(((OperatorSource) operator.getSourceB()).getOperator());
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
      } else if (operator.getType() == OperatorType.MarkJoin
          || operator.getType() == OperatorType.SingleJoin) {
        leftFilter = LogicalFilterUtils.getSubFilterFromPatterns(filter.copy(), leftPatterns);
        rightFilter = LogicalFilterUtils.getSubFilterFromPatterns(filter.copy(), rightPatterns);
        if (LogicalFilterUtils.getPathsFromFilter(leftFilter).stream()
            .anyMatch(path -> path.startsWith(MarkJoin.MARK_PREFIX))) {
          leftFilter = new BoolFilter(true);
        }
        if (LogicalFilterUtils.getPathsFromFilter(rightFilter).stream()
            .anyMatch(path -> path.startsWith(MarkJoin.MARK_PREFIX))) {
          rightFilter = new BoolFilter(true);
        }
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

      // 然后进行判断，如果左右的filter跟原filter不一致，说明需要超集下推，原filter要保留在原位置。如果filter带有*，也需要超集下推
      if (needRemain.contains(operator.getType())
          && (!leftFilter.equals(filter)
              || !rightFilter.equals(filter)
              || filterContainsStar(filter))) {
        remainFilters.add(filter);
      } else if (!needRemain.contains(operator.getType())
          && !leftFilter.equals(filter)
          && !rightFilter.equals(filter)) {
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

    // 如果OuterJoin的某侧下推，说明filter可以把那一侧的null行都过滤掉，那么OuterJoin会进行相应转换。
    // 例如FULL OUTER JOIN，如果左侧下推，那么左侧NULL行都会被过滤掉，那么就变成了LEFT OUTER JOIN
    // 例如LEFT OUTER JOIN，如果右侧下推，那么右侧NULL行都会被过滤掉，那么就变成了INNER JOIN

    select.setFilter(new AndFilter(remainFilters));
    if (!leftFilters.isEmpty()) {
      Select leftSelect =
          new Select(operator.getSourceA(), new AndFilter(leftFilters), select.getTagFilter());
      operator.setSourceA(new OperatorSource(leftSelect));

      if (operator.getType() == OperatorType.OuterJoin) {
        OuterJoin outerJoin = (OuterJoin) operator;
        if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
          operator =
              new InnerJoin(
                  outerJoin.getSourceA(),
                  outerJoin.getSourceB(),
                  outerJoin.getPrefixA(),
                  outerJoin.getPrefixB(),
                  outerJoin.getFilter(),
                  outerJoin.getJoinColumns(),
                  outerJoin.isNaturalJoin(),
                  outerJoin.getJoinAlgType(),
                  outerJoin.getExtraJoinPrefix());
        } else if (outerJoin.getOuterJoinType() == OuterJoinType.FULL) {
          outerJoin.setOuterJoinType(OuterJoinType.LEFT);
        }
      }
    }

    if (!rightFilters.isEmpty()) {
      Select rightSelect =
          new Select(operator.getSourceB(), new AndFilter(rightFilters), select.getTagFilter());
      operator.setSourceB(new OperatorSource(rightSelect));

      if (operator.getType() == OperatorType.OuterJoin) {
        OuterJoin outerJoin = (OuterJoin) operator;
        if (outerJoin.getOuterJoinType() == OuterJoinType.LEFT) {
          operator =
              new InnerJoin(
                  outerJoin.getSourceA(),
                  outerJoin.getSourceB(),
                  outerJoin.getPrefixA(),
                  outerJoin.getPrefixB(),
                  outerJoin.getFilter(),
                  outerJoin.getJoinColumns(),
                  outerJoin.isNaturalJoin(),
                  outerJoin.getJoinAlgType(),
                  outerJoin.getExtraJoinPrefix());
        } else if (outerJoin.getOuterJoinType() == OuterJoinType.FULL) {
          outerJoin.setOuterJoinType(OuterJoinType.RIGHT);
        }
      }
    }
    select.setSource(new OperatorSource(operator));

    if (!remainFilters.isEmpty()) {
      selectMap.put(select, select.getFilter().toString());
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

  private static boolean filterContainsStar(Filter filter) {
    boolean[] containsStar = new boolean[1];
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
            containsStar[0] |= filter.getPath().contains("*");
          }

          @Override
          public void visit(PathFilter filter) {
            containsStar[0] |= filter.getPathA().contains("*");
            containsStar[0] |= filter.getPathB().contains("*");
          }

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {
            containsStar[0] |=
                ExprUtils.getPathFromExpr(filter.getExpressionA()).stream()
                    .anyMatch(path -> path.contains("*"));
            containsStar[0] |=
                ExprUtils.getPathFromExpr(filter.getExpressionB()).stream()
                    .anyMatch(path -> path.contains("*"));
          }
        });
    return containsStar[0];
  }
}
