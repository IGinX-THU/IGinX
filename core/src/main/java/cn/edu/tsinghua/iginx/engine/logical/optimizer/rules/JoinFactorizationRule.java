package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/*
   可以将如下的查询优化：
   SELECT t1.c1, t2.c2
   FROM   t1, t2, t3
   WHERE  t1.c1 = t2.c1
   AND    t1.c1 > 1
   AND    t2.c2 = 2
   AND    t2.c2 = t3.c2
   UNION ALL
   SELECT t1.c1, t2.c2
   FROM   t1, t2, t4
   WHERE  t1.c1 = t2.c1
   AND    t1.c1 > 1
   AND    t2.c3 = t4.c3

   优化为：
   SELECT t1.c1, VW_JF_1.item_2
   FROM   t1, (SELECT t2.c1 item_1, t2.c2 item_2
               FROM   t2, t3
               WHERE  t2.c2 = t3.c2
               AND    t2.c2 = 2
               UNION ALL
               SELECT t2.c1 item_1, t2.c2 item_2
               FROM   t2, t4
               WHERE  t2.c3 = t4.c3) VW_JF_1
   WHERE  t1.c1 = VW_JF_1.item_1
   AND    t1.c1 > 1

   这可以减少一次对t1.*的扫描，而且能够为Join Reorder提供更多的选择
*/

public class JoinFactorizationRule extends Rule {

  private static class InstanceHolder {
    private static final JoinFactorizationRule INSTANCE = new JoinFactorizationRule();
  }

  public static JoinFactorizationRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected JoinFactorizationRule() {
    /*
     * we want to match the topology like:
     *            Union
     *              |
     *             any
     */
    super("JoinFactorizationRule", operand(Union.class, any()));
  }

  private class JoinBranch {
    private List<String> path;
    private Operator operator;

    private List<Filter> filters;

    public JoinBranch(List<String> path, Operator operator) {
      this.path = path;
      this.operator = operator;
      filters = new ArrayList<>();
    }

    public List<String> getPath() {
      return path;
    }

    public List<Filter> getFilters() {
      return filters;
    }

    public void addFilter(Filter filter) {
      filters.add(filter);
    }

    public Operator getOperator() {
      return operator;
    }
  }

  public boolean matches(RuleCall call) {
    Union union = (Union) call.getMatchedRoot();

    // 判断是否是Union all
    if (union.isDistinct()) {
      return false;
    }

    // 如果Union分支有Distinct，那么不进行优化
    if (hasDistinct(call, call.getChildrenIndex().get(union).get(0))
        || hasDistinct(call, call.getChildrenIndex().get(union).get(1))) {
      return false;
    }

    // 接下来我们获取Union左右两个分支的Join中每个分支以及对应的paths
    List<JoinBranch> leftBranches = getJoinBranch(call, call.getChildrenIndex().get(union).get(0));
    List<JoinBranch> rightBranches = getJoinBranch(call, call.getChildrenIndex().get(union).get(1));
    if (leftBranches == null || rightBranches == null) {
      return false;
    }

    // 现在看看是否有相同的paths,如果有的话再看他们的分支是否也是相同的
    List<Pair<JoinBranch, JoinBranch>> matchedBranches = new ArrayList<>();
    for (JoinBranch leftBranch : leftBranches) {
      for (JoinBranch rightBranch : rightBranches) {
        if (leftBranch.getPath().equals(rightBranch.getPath())
            && isSameBranch(call, leftBranch.getOperator(), rightBranch.getOperator())) {
          matchedBranches.add(new Pair<>(leftBranch, rightBranch));
        }
      }
    }

    // 如果有相同的列，那么再比较他们在两个分支上相关的谓词是否相同，如果不同则排除掉
    Select leftSelect = getFirstSelect(call, call.getChildrenIndex().get(union).get(0));
    Select rightSelect = getFirstSelect(call, call.getChildrenIndex().get(union).get(1));
    List<Filter> leftFilters = LogicalFilterUtils.splitFilter(leftSelect.getFilter());
    List<Filter> rightFilters = LogicalFilterUtils.splitFilter(rightSelect.getFilter());

    matchedBranches =
        matchedBranches.stream()
            .filter(
                pair -> {
                  List<String> paths = pair.k.getPath();

                  // leftFilters中每一个包含了paths的filter,都要在rightFilters中找到一个相同的,rightFilters也同理
                  Set<Filter> filterSet = new HashSet<>();
                  for (Filter leftFilter : leftFilters) {
                    Set<String> pathsInLeftFilter =
                        LogicalFilterUtils.getPathsFromFilter(leftFilter);
                    if (pathsInLeftFilter.retainAll(paths) && pathsInLeftFilter.isEmpty()) {
                      filterSet.add(leftFilter);
                      continue;
                    }
                    for (Filter rightFilter : rightFilters) {
                      if (filterSet.contains(rightFilter)) {
                        continue;
                      }
                      Set<String> pathsInRightFilter =
                          LogicalFilterUtils.getPathsFromFilter(rightFilter);
                      if (pathsInRightFilter.retainAll(paths) && pathsInRightFilter.isEmpty()) {
                        filterSet.add(rightFilter);
                        continue;
                      }

                      if (leftFilter.equals(rightFilter)) {
                        filterSet.add(leftFilter);
                        filterSet.add(rightFilter);
                        pair.k.addFilter(leftFilter);
                        pair.v.addFilter(rightFilter);
                      }
                    }
                  }
                  return filterSet.containsAll(leftFilters) && filterSet.containsAll(rightFilters);
                })
            .collect(Collectors.toList());

    call.setContext(matchedBranches);
    return !matchedBranches.isEmpty();
  }

  public void onMatch(RuleCall call) {
    Union union = (Union) call.getMatchedRoot();
  }

  /**
   * 找到Binary节点前的第一个Select节点，因为Binary节点后的Select节点可能是子查询中的，不是我们想要的
   *
   * @param ruleCall 上下文
   * @param operator 根节点
   * @return 第一个Select节点
   */
  private Select getFirstSelect(RuleCall ruleCall, Operator operator) {
    Map<Operator, List<Operator>> childrenIndex = ruleCall.getChildrenIndex();
    Queue<Operator> queue = new LinkedList<>();
    queue.add(operator);
    while (!queue.isEmpty()) {
      Operator curOp = queue.poll();
      if (curOp.getType() == OperatorType.Select) {
        return (Select) curOp;
      }
      List<Operator> children = childrenIndex.get(curOp);
      if (children != null && children.size() == 1) {
        queue.addAll(children);
      }
    }
    return null;
  }

  /**
   * 判断在Binary节点前是否有distinct，如果有则返回true，Binary节点后的Distinct可能是子查询中的，不是我们想要的。
   *
   * @param ruleCall 上下文
   * @param operator 根节点
   * @return 是否有Distinct
   */
  private boolean hasDistinct(RuleCall ruleCall, Operator operator) {
    Map<Operator, List<Operator>> childrenIndex = ruleCall.getChildrenIndex();
    Queue<Operator> queue = new LinkedList<>();
    queue.add(operator);
    while (!queue.isEmpty()) {
      Operator curOp = queue.poll();
      if (curOp.getType() == OperatorType.Distinct) {
        return true;
      }
      List<Operator> children = childrenIndex.get(curOp);
      if (children != null && children.size() == 1) {
        queue.addAll(children);
      }
    }
    return false;
  }

  private List<JoinBranch> getJoinBranch(RuleCall ruleCall, Operator operator) {
    // 先向下找到第一个InnerJoin/CrossJoin节点
    while (operator.getType() != OperatorType.InnerJoin
        && operator.getType() != OperatorType.CrossJoin) {
      if (OperatorType.isBinaryOperator(operator.getType())) {
        return null;
      }
      operator = ruleCall.getChildrenIndex().get(operator).get(0);
    }

    // 然后获取这一系列Join下面的第一个非Join节点
    Queue<Operator> queue = new LinkedList<>();
    List<JoinBranch> branches = new ArrayList<>();
    queue.add(operator);
    while (!queue.isEmpty()) {
      Operator curOp = queue.poll();
      if (curOp.getType() == OperatorType.InnerJoin && curOp.getType() == OperatorType.CrossJoin) {
        queue.addAll(ruleCall.getChildrenIndex().get(curOp));
      } else if (OperatorType.isUnaryOperator(curOp.getType())) {
        branches.add(new JoinBranch(OperatorUtils.findPathList(curOp), curOp));
      } else {
        return null;
      }
    }

    return branches;
  }

  private boolean isSameBranch(RuleCall ruleCall, Operator operator1, Operator operator2) {
    // 先比较两个节点是否相同
    if (!operator1.equals(operator2)) {
      return false;
    }

    // 如果两个节点相同，那么我们需要比较他们的子节点是否也相同
    List<Operator> children1 = ruleCall.getChildrenIndex().get(operator1);
    List<Operator> children2 = ruleCall.getChildrenIndex().get(operator2);
    if (children1 == null && children2 == null) {
      return true;
    } else if (children1 == null || children2 == null) {
      return false;
    } else if (children1.size() != children2.size()) {
      return false;
    }
    return IntStream.range(0, children1.size())
        .allMatch(i -> isSameBranch(ruleCall, children1.get(i), children2.get(i)));
  }
}
