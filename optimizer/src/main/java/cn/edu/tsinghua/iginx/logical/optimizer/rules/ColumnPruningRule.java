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

import static cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils.*;

import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.logical.optimizer.core.RuleCall;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnPruningRule extends Rule {

  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnPruningRule.class);

  private static final class InstanceHolder {
    static final ColumnPruningRule INSTANCE = new ColumnPruningRule();
  }

  public static ColumnPruningRule getInstance() {
    return InstanceHolder.INSTANCE;
  }

  protected ColumnPruningRule() {
    /*
     * we want to match the topology like:
     *         Any
     */
    // 匹配任意操作符，但由于策略是ONCE，所以只会匹配到树的顶端
    super("ColumnPruningRule", operand(AbstractOperator.class, any()), RuleStrategy.ONCE);
  }

  @Override
  public boolean matches(RuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RuleCall call) {
    /*
     大致优化思路是，自顶向下地遍历树，对于每个节点，我们都会收集它的列，然后递归地处理它的子节点。
     对于不同的节点，我们会进行根据列收集进行裁剪或将列加入收集中。
    */
    collectColumns(call.getMatchedRoot(), new HashSet<>(), null, new ArrayList<>());
  }

  private void collectColumns(
      Operator operator,
      Set<String> columns,
      TagFilter tagFilter,
      List<Operator> visitedOperators) {
    if (OperatorType.isUnaryOperator(operator.getType())) {
      List<String> newColumnList = null;
      List<FunctionCall> functionCallList = null;

      if (operator.getType() == OperatorType.Select) {
        // 提取filter中的列
        Select select = (Select) operator;
        Filter filter = select.getFilter();
        newColumnList = FilterUtils.getAllPathsFromFilter(filter);
      } else if (operator.getType() == OperatorType.Reorder) {
        Reorder reorder = (Reorder) operator;
        if (columns.isEmpty()
            || (hasWildCard(columns) && PathUtils.checkCoverage(columns, reorder.getPatterns()))) {
          columns.clear();
          columns.addAll(reorder.getPatterns());
        } else {
          ArrayList<String> patterns = new ArrayList<>(columns);
          Collections.sort(patterns);
          reorder.setPatterns(patterns);
        }
      } else if (operator.getType() == OperatorType.Project) {
        Project project = (Project) operator;
        if (columns.isEmpty()
            || hasWildCard(columns) && PathUtils.checkCoverage(columns, project.getPatterns())) {
          columns.clear();
          columns.addAll(project.getPatterns());
          tagFilter = project.getTagFilter();
        } else {
          ArrayList<String> patterns = new ArrayList<>(columns);
          Collections.sort(patterns);
          project.setPatterns(patterns);
          if (tagFilter == null) {
            tagFilter = project.getTagFilter();
          } else if (!Objects.equals(tagFilter.toString(), project.getTagFilter().toString())) {
            tagFilter = new AndTagFilter(Arrays.asList(tagFilter, project.getTagFilter()));
          }
        }
      } else if (operator.getType() == OperatorType.Rename) {
        Rename rename = (Rename) operator;
        Map<String, String> aliasMap = rename.getAliasMap();
        columns =
            new HashSet<>(PathUtils.recoverRenamedPatterns(aliasMap, new ArrayList<>(columns)));

      } else if (operator.getType() == OperatorType.GroupBy) {
        GroupBy groupBy = (GroupBy) operator;
        newColumnList = groupBy.getGroupByCols();
        functionCallList = groupBy.getFunctionCallList();
      } else if (operator.getType() == OperatorType.Downsample) {
        Downsample downsample = (Downsample) operator;
        functionCallList = downsample.getFunctionCallList();
      } else if (operator.getType() == OperatorType.Sort) {
        Sort sort = (Sort) operator;
        for (String column : sort.getSortByCols()) {
          if (!column.equalsIgnoreCase(Constants.KEY)) {
            columns.add(column);
          }
        }
      } else if (operator.getType() == OperatorType.AddSchemaPrefix) {
        String prefix = ((AddSchemaPrefix) operator).getSchemaPrefix();
        if (prefix != null) {
          Set<String> newColumns = new HashSet<>();
          for (String column : columns) {
            if (column.startsWith(prefix + ".")) {
              newColumns.add(column.substring(prefix.length() + 1));
            } else {
              newColumns.add(column);
            }
          }
          columns = newColumns;
        }
      } else if (operator.getType() == OperatorType.SetTransform) {
        SetTransform setTransform = (SetTransform) operator;
        functionCallList = setTransform.getFunctionCallList();
      } else if (operator.getType() == OperatorType.RowTransform) {
        RowTransform rowTransform = (RowTransform) operator;
        functionCallList = rowTransform.getFunctionCallList();
      } else if (operator.getType() == OperatorType.MappingTransform) {
        MappingTransform mappingTransform = (MappingTransform) operator;
        functionCallList = mappingTransform.getFunctionCallList();
      }

      if (newColumnList != null) {
        Set<String> removeCoveredColumnList = getNewColumns(columns, newColumnList);

        // 如果有新列，且该节点的父节点不是Project,
        // 说明我们应该在该节点上方加入一个Project节点，来裁剪掉不需要的列
        if (!visitedOperators.isEmpty() && !removeCoveredColumnList.isEmpty()) {
          Operator parent = visitedOperators.get(visitedOperators.size() - 1);
          if (parent.getType() != OperatorType.Project
              && parent.getType() != OperatorType.Reorder) {
            Project project =
                new Project(new OperatorSource(operator), new ArrayList<>(columns), tagFilter);
            if (OperatorType.isUnaryOperator(parent.getType())) {
              ((UnaryOperator) parent).setSource(new OperatorSource(project));
            } else if (OperatorType.isBinaryOperator(parent.getType())) {
              if (((OperatorSource) ((BinaryOperator) parent).getSourceA()).getOperator()
                  == operator) {
                ((BinaryOperator) parent).setSourceA(new OperatorSource(project));
              } else {
                ((BinaryOperator) parent).setSourceB(new OperatorSource(project));
              }
            }
          }
        }

        columns.addAll(removeCoveredColumnList);
      }
      if (functionCallList != null) {
        changeColumnsFromFunctionCallList(functionCallList, columns);
      }

      // 递归处理下一个节点
      visitedOperators.add(operator);
      if (((UnaryOperator) operator).getSource() instanceof FragmentSource) {
        return;
      }
      collectColumns(
          ((OperatorSource) ((UnaryOperator) operator).getSource()).getOperator(),
          columns,
          tagFilter,
          visitedOperators);

    } else if (OperatorType.isBinaryOperator(operator.getType())) {
      // 下面是处理BinaryOperator的情况，其中只有Join操作符需要处理

      Set<String> leftColumns = new HashSet<>();
      Set<String> rightColumns = new HashSet<>();
      if (OperatorType.isJoinOperator(operator.getType())) {
        String PrefixA = null, PrefixB = null;
        Filter filter = null;
        boolean isNaturalJoin = false;
        List<String> extraJoinPath = new ArrayList<>();

        if (operator.getType() == OperatorType.InnerJoin) {
          InnerJoin innerJoin = (InnerJoin) operator;
          PrefixA = innerJoin.getPrefixA();
          PrefixB = innerJoin.getPrefixB();
          filter = innerJoin.getFilter();
          isNaturalJoin = innerJoin.isNaturalJoin();
          extraJoinPath.addAll(innerJoin.getExtraJoinPrefix());
        } else if (operator.getType() == OperatorType.OuterJoin) {
          OuterJoin outerJoin = (OuterJoin) operator;
          PrefixA = outerJoin.getPrefixA();
          PrefixB = outerJoin.getPrefixB();
          filter = outerJoin.getFilter();
          isNaturalJoin = outerJoin.isNaturalJoin();
        } else if (operator.getType() == OperatorType.CrossJoin) {
          CrossJoin crossJoin = (CrossJoin) operator;
          PrefixA = crossJoin.getPrefixA();
          PrefixB = crossJoin.getPrefixB();
          extraJoinPath.addAll(crossJoin.getExtraJoinPrefix());
        } else if (operator.getType() == OperatorType.MarkJoin) {
          MarkJoin markJoin = (MarkJoin) operator;
          // 在columns中找到&mark的列，删除，这是由于MarkFilter引入的列，不用于后续计算
          columns.removeIf(column -> column.startsWith(MarkJoin.MARK_PREFIX));
          // MarkJoin的左侧是普通子树，右侧是WHERE关联子查询子树，我们只处理左侧，裁剪右侧的列会导致语义变化
          filter = markJoin.getFilter();
        } else if (operator.getType() == OperatorType.SingleJoin) {
          SingleJoin singleJoin = (SingleJoin) operator;
          filter = singleJoin.getFilter();
        }

        if (isNaturalJoin) {
          // NaturalJoin可能会用到所有的列，因此我们直接返回，不继续优化
          return;
        }

        if (filter != null) {
          columns.addAll(getNewColumns(columns, FilterUtils.getAllPathsFromFilter(filter)));
        }

        // 如果是MarkJoin或SingleJoin,要把columns中右侧的列去掉
        if (operator.getType() == OperatorType.MarkJoin
            || operator.getType() == OperatorType.SingleJoin) {
          List<String> rightPatterns =
              OperatorUtils.getPatternFromOperatorChildren(
                  ((OperatorSource) ((BinaryOperator) operator).getSourceB()).getOperator(),
                  new ArrayList<>());
          leftColumns.addAll(getNewColumns(rightPatterns, columns));
        }

        // 将columns中的列名分成以PrefixA和PrefixB开头的两部分
        if (PrefixA != null && PrefixB != null) {
          for (String column : columns) {
            if (column.contains("*")
                && (!OperatorUtils.covers(PrefixA + ".*", column)
                    && OperatorUtils.covers(column, PrefixA + ".*"))
                && (!OperatorUtils.covers(PrefixB + ".*", column)
                    && OperatorUtils.covers(column, PrefixB + ".*"))) {
              /*
              如果现有的列中包含通配符*，这个通配符会受到当前Join算子的限制，不再能下推，再推下去会导致取的列过多
              例如最顶上Project的test.*，但是JOIN算子左右两侧是test.a.*和test.b.*，这是的test.*实际上取的列是test.a.*和test.b.*的并集
              但如果将test.*下推到JOIN算子后，会取到test.a.*和test.b.*之外的列，因此我们丢弃所有列，让子树重新计算
              */
              leftColumns.clear();
              rightColumns.clear();
              leftColumns.add(PrefixA + ".*");
              rightColumns.add(PrefixB + ".*");

              // 以及不在左右Prefix的列要加入左子树中
              for (String c : columns) {
                if (!c.startsWith(PrefixA + ".") && !c.startsWith(PrefixB + "."))
                  leftColumns.add(c);
              }
              break;
            }

            if (OperatorUtils.covers(PrefixA + ".*", column)
                || OperatorUtils.covers(column, PrefixA + ".*")) {
              leftColumns.add(column);
            } else if (OperatorUtils.covers(PrefixB + ".*", column)
                || OperatorUtils.covers(column, PrefixB + ".*")) {
              rightColumns.add(column);
            } else {
              // 如果没有匹配的话，可能是连续JOIN的情况，即SELECT t1.*, t2.*, t3.* FROM t1 JOIN t2 JOIN t3;(这里省略了ON条件）
              // 目前看来第一个InnerJoin只有t1、t2的信息，t3的InnerJoin放在左子树(PrefixA)中
              leftColumns.add(column);
            }
          }

          if (!extraJoinPath.isEmpty()) {
            leftColumns.addAll(getNewColumns(leftColumns, extraJoinPath));
            rightColumns.addAll(getNewColumns(rightColumns, extraJoinPath));
          }
        }

      } else if (OperatorType.isSetOperator(operator.getType())) {
        Pair<List<String>, List<String>> orderPair = getSetOperatorOrder(operator);
        List<String> leftOrder = orderPair.getK(), rightOrder = orderPair.getV();
        // 如果左侧将要替换的columns和右侧order有*的话，我们无法确定具体*对应的列数，因此不裁剪，继续往下递归
        if (hasPatternOperator(visitedOperators)
            && !(columns.isEmpty() || hasWildCard(columns) || hasWildCard(rightOrder))) {
          // SetOperator要求左右两侧的列数相同，我们只对左侧进行列裁剪，然后右侧相应减少对应的列（有顺序要求）
          List<String> newLeftOrder = getOrderListFromColumns(columns, leftOrder);
          List<String> newRightOrder = new ArrayList<>();
          // 去除右侧，索引超出左侧的列
          for (int i = 0; i < newLeftOrder.size(); i++) {
            newRightOrder.add(rightOrder.get(i));
          }
          setSetOperatorOrder(operator, newLeftOrder, newRightOrder);
          leftColumns.addAll(newLeftOrder);
          rightColumns.addAll(newRightOrder);
        } else {
          leftColumns.addAll(leftOrder);
          rightColumns.addAll(rightOrder);
        }
      } else {
        leftColumns.addAll(columns);
        rightColumns.addAll(columns);
      }

      // 递归处理下一个节点
      visitedOperators.add(operator);
      collectColumns(
          ((OperatorSource) ((BinaryOperator) operator).getSourceA()).getOperator(),
          leftColumns,
          tagFilter,
          visitedOperators);
      collectColumns(
          ((OperatorSource) ((BinaryOperator) operator).getSourceB()).getOperator(),
          rightColumns,
          tagFilter,
          new ArrayList<>(visitedOperators));
    }
  }

  /**
   * 从FunctionCallList中将columns中函数结果列还原为参数列
   *
   * @param functionCallList 函数列表
   * @param columns 优化过程中的列列表，我们直接在这个列表上进行修改
   */
  private void changeColumnsFromFunctionCallList(
      List<FunctionCall> functionCallList, Set<String> columns) {
    for (FunctionCall functionCall : functionCallList) {
      String functionName = FunctionUtils.getFunctionName(functionCall.getFunction());
      List<String> paths = functionCall.getParams().getPaths();
      boolean isPyUDF;
      try {
        isPyUDF = FunctionUtils.isPyUDF(functionName);
      } catch (Exception e) {
        isPyUDF = false;
      }

      if (functionName.equals(First.FIRST) || functionName.equals(Last.LAST)) {
        // First和Last的结果列是path和value
        columns.remove("path");
        columns.remove("value");
        columns.addAll(paths);
      } else if (functionName.equals(ArithmeticExpr.ARITHMETIC_EXPR)) {
        String functionStr = functionCall.getParams().getExpr().getColumnName();
        columns.remove(functionStr);
        columns.addAll(ExprUtils.getPathFromExpr(functionCall.getParams().getExpr()));
      } else if (isPyUDF) {
        // UDF结果列括号内可以随意命名，因此我们识别columns中所有开头为UDF的列，不识别括号内的内容
        String prefix = functionName + "(";
        Set<String> newColumns = new HashSet<>();
        for (String column : columns) {
          if (column.startsWith(prefix)) {
            newColumns.addAll(paths);
          } else {
            newColumns.add(column);
          }
        }
        columns.clear();
        columns.addAll(newColumns);
      } else {
        String functionStr = functionName + "(" + String.join(", ", paths) + ")";
        if (functionCall.getParams().isDistinct()) {
          functionStr = functionName + "(distinct " + String.join(", ", paths) + ")";
        }
        columns.addAll(paths);
        if (columns.contains(functionStr)) {
          columns.remove(functionStr);
        } else {
          LOGGER.warn("FunctionCallList中的函数结果列不在columns中");
        }
      }
    }
  }

  /**
   * 检查列中是否有通配符
   *
   * @param columns 列列表
   * @return 是否有通配符
   */
  private static boolean hasWildCard(Collection<String> columns) {
    for (String column : columns) {
      if (column.contains(Constants.ALL_PATH)) {
        return true;
      }
    }
    return false;
  }

  static final Set<OperatorType> patternOperators =
      new HashSet<>(
          Arrays.asList(
              OperatorType.Project,
              OperatorType.GroupBy,
              OperatorType.Downsample,
              OperatorType.SetTransform,
              OperatorType.RowTransform,
              OperatorType.MappingTransform));

  /**
   * 检查父节点（递归）中是否含有能决定Pattern的操作符：Project、GroupBy、DownSample、Set/Row/MappingTransform
   *
   * @param visitedOperators 已经访问过的操作符
   * @return 是否含有能决定Pattern的操作符
   */
  private static boolean hasPatternOperator(List<Operator> visitedOperators) {
    for (Operator operator : visitedOperators) {
      if (patternOperators.contains(operator.getType())) {
        return true;
      }
    }
    return false;
  }

  /**
   * 裁剪order中的列，如果order中的列在columns中，保留，如果带*，则找到columns中能匹配的列，并按字典序排序
   *
   * @param columns 列列表
   * @param order SetOperator中提取的order列表
   * @return 裁剪后的order列表
   */
  private static List<String> getOrderListFromColumns(Set<String> columns, List<String> order) {
    List<String> result = new ArrayList<>();
    for (String orderColumn : order) {
      if (columns.contains(orderColumn)) {
        result.add(orderColumn);
      } else if (orderColumn.contains("*")) {
        List<String> columnsList = new ArrayList<>();
        for (String column : columns) {
          if (StringUtils.match(column, orderColumn)) {
            columnsList.add(column);
          }
        }
        Collections.sort(columnsList);
        result.addAll(columnsList);
      }
    }
    return result;
  }

  private static void setSetOperatorOrder(
      Operator operator, List<String> leftOrder, List<String> rightOrder) {
    if (operator.getType() == OperatorType.Union) {
      Union union = (Union) operator;
      union.setLeftOrder(leftOrder);
      union.setRightOrder(rightOrder);
    } else if (operator.getType() == OperatorType.Intersect) {
      Intersect intersect = (Intersect) operator;
      intersect.setLeftOrder(leftOrder);
      intersect.setRightOrder(rightOrder);
    } else if (operator.getType() == OperatorType.Except) {
      Except except = (Except) operator;
      except.setLeftOrder(leftOrder);
      except.setRightOrder(rightOrder);
    } else {
      throw new IllegalArgumentException("Operator is not a SetOperator");
    }
  }

  private static Pair<List<String>, List<String>> getSetOperatorOrder(Operator operator) {
    if (operator.getType() == OperatorType.Union) {
      Union union = (Union) operator;
      return new Pair<>(union.getLeftOrder(), union.getRightOrder());
    } else if (operator.getType() == OperatorType.Intersect) {
      Intersect intersect = (Intersect) operator;
      return new Pair<>(intersect.getLeftOrder(), intersect.getRightOrder());
    } else if (operator.getType() == OperatorType.Except) {
      Except except = (Except) operator;
      return new Pair<>(except.getLeftOrder(), except.getRightOrder());
    } else {
      throw new IllegalArgumentException("Operator is not a SetOperator");
    }
  }

  /**
   * 去除newColumnsList中被columns覆盖的列，并返回
   *
   * @param columns
   * @param newColumnList
   * @return 去除了被覆盖的列后的列列表
   */
  private static Set<String> getNewColumns(
      Collection<String> columns, Collection<String> newColumnList) {
    // 检测newColumnList中的列是否有被原columns覆盖的列
    Set<String> removeCoveredColumnList = new HashSet<>(); // 去除了被覆盖的列后的列列表
    for (String newColumn : newColumnList) {
      boolean covered = false;
      for (String c : columns) {
        if (OperatorUtils.covers(c, newColumn)) {
          covered = true;
          break;
        }
      }
      if (!covered) {
        removeCoveredColumnList.add(newColumn);
      }
    }
    return removeCoveredColumnList;
  }
}
