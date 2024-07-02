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
package cn.edu.tsinghua.iginx.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
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

public class FilterFragmentOptimizer implements Optimizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterFragmentOptimizer.class);

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static FilterFragmentOptimizer instance;

  private static final List<Predicate<Operator>> onlyHasProjectAndJoinByKeyAndUnionConditions =
      new ArrayList<>();

  static {
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(op -> op.getType() == OperatorType.Project);
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(
        op -> op.getType() == OperatorType.Join && ((Join) op).getJoinBy().equals(Constants.KEY));
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(op -> op.getType() == OperatorType.Union);
    onlyHasProjectAndJoinByKeyAndUnionConditions.add(op -> op.getType() == OperatorType.PathUnion);
  }

  private FilterFragmentOptimizer() {}

  public static FilterFragmentOptimizer getInstance() {
    if (instance == null) {
      synchronized (FilterFragmentOptimizer.class) {
        if (instance == null) {
          instance = new FilterFragmentOptimizer();
        }
      }
    }
    return instance;
  }

  @Override
  public Operator optimize(Operator root) {
    // only optimize query
    if (root.getType() == OperatorType.CombineNonQuery
        || root.getType() == OperatorType.ShowColumns) {
      return root;
    }

    List<Select> selectOperatorList = new ArrayList<>();
    OperatorUtils.findSelectOperators(selectOperatorList, root);

    if (selectOperatorList.isEmpty()) {
      LOGGER.info("There is no filter in logical tree.");
      return root;
    }

    for (Select selectOperator : selectOperatorList) {
      filterFragmentByTimeRange(selectOperator);
    }
    return root;
  }

  private void filterFragmentByTimeRange(Select selectOperator) {
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

    // 因为该optimizer是针对key范围进行优化，所以如果select operator没有对key进行过滤，那么就不需要进行优化，直接返回
    if (keyRanges.isEmpty()) {
      return;
    }

    // 如果Select Operator的子树中包含非Project、Join(ByKey)、Union、PathUnion节点，那么无法进行优化，直接返回
    // 无法优化的情况有(1)Select Operator下含有子查询，(2)含有OUTER JOIN、INNER JOIN等会消除Key列的JOIN操作，在此排除。
    if (!onlyHasProjectAndJoinByKeyAndUnion(
        ((OperatorSource) (selectOperator.getSource())).getOperator())) {
      return;
    }

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
      selectOperator.setSource(new OperatorSource(root));
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

  /** 判断子树中是否含有非Project、Join(ByKey)、Union、PathUnion节点 */
  private static boolean onlyHasProjectAndJoinByKeyAndUnion(Operator operator) {
    boolean res =
        onlyHasProjectAndJoinByKeyAndUnionConditions.stream()
            .anyMatch(condition -> condition.test(operator));

    if (!res) {
      return false;
    }

    // dfs to find select operator.
    if (OperatorType.isUnaryOperator(operator.getType())) {
      UnaryOperator unaryOp = (UnaryOperator) operator;
      Source source = unaryOp.getSource();
      if (source.getType() != SourceType.Fragment) {
        res = res && onlyHasProjectAndJoinByKeyAndUnion(((OperatorSource) source).getOperator());
      }
    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      BinaryOperator binaryOperator = (BinaryOperator) operator;
      Source sourceA = binaryOperator.getSourceA();
      Source sourceB = binaryOperator.getSourceB();
      if (sourceA.getType() != SourceType.Fragment) {
        res = res && onlyHasProjectAndJoinByKeyAndUnion(((OperatorSource) sourceA).getOperator());
      }
      if (sourceB.getType() != SourceType.Fragment) {
        res = res && onlyHasProjectAndJoinByKeyAndUnion(((OperatorSource) sourceB).getOperator());
      }
    } else {
      MultipleOperator multipleOperator = (MultipleOperator) operator;
      List<Source> sources = multipleOperator.getSources();
      for (Source source : sources) {
        if (source.getType() != SourceType.Fragment) {
          res = res && onlyHasProjectAndJoinByKeyAndUnion(((OperatorSource) source).getOperator());
        }
      }
    }

    return res;
  }
}
