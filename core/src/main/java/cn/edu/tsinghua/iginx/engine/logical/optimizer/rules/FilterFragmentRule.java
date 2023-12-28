package cn.edu.tsinghua.iginx.engine.logical.optimizer.rules;

import static cn.edu.tsinghua.iginx.metadata.utils.FragmentUtils.keyFromColumnsIntervalToKeyInterval;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.MetaManagerWrapper;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterFragmentRule extends Rule {
  private static final class InstanceHolder {
    static final FilterFragmentRule INSTANCE = new FilterFragmentRule();
  }

  public static FilterFragmentRule getInstance() {
    return FilterFragmentRule.InstanceHolder.INSTANCE;
  }

  protected FilterFragmentRule() {
    /*
     * we want to match the topology like:
     *          Select
     *            |
     *           Any
     */
    super("FilterFragmentRule", operand(Select.class, any()));
  }

  private static final Logger logger = LoggerFactory.getLogger(FilterFragmentRule.class);

  private static final IMetaManager metaManager = MetaManagerWrapper.getInstance();

  private static final List<Predicate<Operator>> onlyHasProjectAndJoinByKeyAndUnionConditions =
      new ArrayList<>();

  static {
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(op -> op.getType() == OperatorType.Project);
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(
        op -> op.getType() == OperatorType.Join && ((Join) op).getJoinBy().equals(Constants.KEY));
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(op -> op.getType() == OperatorType.Union);
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(op -> op.getType() == OperatorType.PathUnion);
  }

  /**
   * 判断是否匹配，如果有以下情况，则不匹配： (1) Select Operator的子树中包含非Project、Join(ByKey)、Union、PathUnion节点，子树包含子查询
   * (2) Select Operator没有keyFilter
   *
   * @param call RuleCall上下文
   * @return 是否匹配
   */
  @Override
  public boolean matches(RuleCall call) {
    Select selectOperator = (Select) call.getMatchedRoot();

    // 如果Select Operator的子树中包含非Project、Join(ByKey)、Union、PathUnion节点，那么无法进行优化，直接返回
    // 无法优化的情况有(1)Select Operator下含有子查询，(2)含有OUTER JOIN、INNER JOIN等会消除Key列的JOIN操作，在此排除。
    final boolean[] res = {true};
    Predicate<Operator> condition =
        operator ->
            onlyHasProjectAndJoinByKeyAndUnionConditions.stream()
                .anyMatch(innerCondition -> innerCondition.test(operator));

    Operator selectChild = ((OperatorSource) selectOperator.getSource()).getOperator();

    selectChild.accept(
        new OperatorVisitor() {
          @Override
          public void visit(UnaryOperator unaryOperator) {
            res[0] = res[0] && condition.test(unaryOperator);
          }

          @Override
          public void visit(BinaryOperator binaryOperator) {
            res[0] = res[0] && condition.test(binaryOperator);
          }

          @Override
          public void visit(MultipleOperator multipleOperator) {
            res[0] = res[0] && condition.test(multipleOperator);
          }
        });

    return res[0];
  }

  @Override
  public void onMatch(RuleCall call) {
    Select selectOperator = (Select) call.getMatchedRoot();
    List<String> pathList = OperatorUtils.findPathList(selectOperator);
    if (pathList.isEmpty()) {
      logger.error("Can not find paths in select operator.");
      return;
    }

    ColumnsInterval columnsInterval =
        new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));
    Map<ColumnsInterval, List<FragmentMeta>> fragmentsByColumnsInterval =
        metaManager.getFragmentMapByColumnsInterval(columnsInterval, true);
    Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
        keyFromColumnsIntervalToKeyInterval(fragmentsByColumnsInterval);
    Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
    List<FragmentMeta> dummyFragments = pair.v;

    Filter filter = selectOperator.getFilter();
    List<KeyRange> keyRanges = ExprUtils.getKeyRangesFromFilter(filter);

    // 将符合Key范围的Fragment Project节点用Join(ByKey)合成一个新的子树并设置为Select Operator的子节点
    List<Operator> unionList = new ArrayList<>();
    fragments.forEach(
        (k, v) -> {
          List<Operator> joinList = new ArrayList<>();
          v.forEach(
              meta -> {
                if (hasTimeRangeOverlap(meta, keyRanges)) {
                  joinList.add(
                      new Project(
                          new FragmentSource(meta), pathList, selectOperator.getTagFilter()));
                }
              });
          Operator operator = OperatorUtils.joinOperatorsByTime(joinList);
          if (operator != null) {
            unionList.add(operator);
          }
        });

    Operator root = OperatorUtils.unionOperators(unionList);
    if (!dummyFragments.isEmpty()) {
      List<Operator> joinList = new ArrayList<>();
      dummyFragments.forEach(
          meta -> {
            if (meta.isValid() && hasTimeRangeOverlap(meta, keyRanges)) {
              joinList.add(
                  new Project(new FragmentSource(meta), pathList, selectOperator.getTagFilter()));
            }
          });
      if (root != null) {
        joinList.add(root);
      }
      root = OperatorUtils.joinOperatorsByTime(joinList);
    }
    if (root != null) {
      call.transformTo(
          new Select(
              new OperatorSource(root), selectOperator.getFilter(), selectOperator.getTagFilter()));
    }
  }

  private boolean hasTimeRangeOverlap(FragmentMeta meta, List<KeyRange> keyRanges) {
    KeyInterval interval = meta.getKeyInterval();
    for (KeyRange range : keyRanges) {
      if (interval.getStartKey() > range.getEndKey()
          || interval.getEndKey() < range.getBeginKey()) {
        // continue
      } else {
        return true;
      }
    }
    return false;
  }
}
