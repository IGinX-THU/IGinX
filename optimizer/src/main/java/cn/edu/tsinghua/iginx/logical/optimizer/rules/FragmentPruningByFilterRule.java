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
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.MetaManagerWrapper;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.utils.FragmentUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FragmentPruningByFilterRule extends Rule {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentPruningByFilterRule.class);

  private static final class InstanceHolder {
    static final FragmentPruningByFilterRule INSTANCE = new FragmentPruningByFilterRule();
  }

  public static FragmentPruningByFilterRule getInstance() {
    return FragmentPruningByFilterRule.InstanceHolder.INSTANCE;
  }

  protected FragmentPruningByFilterRule() {
    /*
     * we want to match the topology like:
     *          Select
     *            |
     *           Any
     */
    super("FragmentPruningByFilterRule", operand(Select.class, any()));
  }

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

    List<String> pathList = OperatorUtils.findPathList(selectOperator);
    List<KeyRange> keyRanges =
        LogicalFilterUtils.getKeyRangesFromFilter(selectOperator.getFilter());

    ColumnsInterval columnsInterval = null;
    if (!pathList.isEmpty()) {
      columnsInterval =
          new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1) + "~");
    }

    final boolean[] hasInvalidFragment = {false};
    ColumnsInterval finalColumnsInterval = columnsInterval;
    selectChild.accept(
        new OperatorVisitor() {
          @Override
          public void visit(UnaryOperator unaryOperator) {
            if (unaryOperator instanceof Project
                && unaryOperator.getSource() instanceof FragmentSource) {
              FragmentMeta fragmentMeta =
                  ((FragmentSource) unaryOperator.getSource()).getFragment();
              hasInvalidFragment[0] =
                  hasInvalidFragment[0]
                      || !fragmentMeta.isValid()
                      || (!keyRanges.isEmpty() && !hasTimeRangeOverlap(fragmentMeta, keyRanges))
                      || (finalColumnsInterval != null
                          && !fragmentMeta.getColumnsInterval().isIntersect(finalColumnsInterval));
            }
          }

          @Override
          public void visit(BinaryOperator binaryOperator) {}

          @Override
          public void visit(MultipleOperator multipleOperator) {}
        });

    return res[0] && hasInvalidFragment[0];
  }

  @Override
  public void onMatch(RuleCall call) {
    Select selectOperator = (Select) call.getMatchedRoot();
    List<String> pathList = OperatorUtils.findPathList(selectOperator);
    if (pathList.isEmpty()) {
      LOGGER.error("Can not find paths in select operator.");
      return;
    }

    ColumnsInterval columnsInterval =
        new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));
    Map<ColumnsInterval, List<FragmentMeta>> fragmentsByColumnsInterval =
        metaManager.getFragmentMapByColumnsInterval(columnsInterval, true);
    Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
        FragmentUtils.keyFromColumnsIntervalToKeyInterval(fragmentsByColumnsInterval);
    Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
    List<FragmentMeta> dummyFragments = pair.v;

    Filter filter = selectOperator.getFilter();
    List<KeyRange> keyRanges = LogicalFilterUtils.getKeyRangesFromFilter(filter);

    // 将符合Key范围的Fragment Project节点用Join(ByKey)合成一个新的子树并设置为Select Operator的子节点
    List<Operator> unionList = new ArrayList<>();
    fragments.forEach(
        (k, v) -> {
          List<Operator> joinList = new ArrayList<>();
          v.forEach(
              meta -> {
                if (hasTimeRangeOverlap(meta, keyRanges)) {
                  ColumnsInterval metaColumnsInterval = meta.getColumnsInterval();
                  List<String> validPaths = new ArrayList<>();
                  for (String path : pathList) {
                    if (metaColumnsInterval.isContainWithoutPrefix(path)) {
                      validPaths.add(path);
                    }
                  }

                  joinList.add(
                      new Project(
                          new FragmentSource(meta), validPaths, selectOperator.getTagFilter()));
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
