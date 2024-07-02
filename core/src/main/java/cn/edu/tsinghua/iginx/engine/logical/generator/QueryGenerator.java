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
package cn.edu.tsinghua.iginx.engine.logical.generator;

import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.getFragmentsByColumnsInterval;
import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.mergeRawData;
import static cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils.translateApply;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.ORDINAL;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr.ARITHMETIC_EXPR;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.chooseJoinAlg;
import static cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType.isNaturalJoin;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.LogicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FromValueExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.CteFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.PathFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.ShowColumnsFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.sql.statement.select.BinarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement.QueryType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryGenerator extends AbstractGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryGenerator.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final QueryGenerator instance = new QueryGenerator();
  private static final FunctionManager functionManager = FunctionManager.getInstance();
  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();
  private final IPolicy policy =
      PolicyManager.getInstance()
          .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

  private QueryGenerator() {
    this.type = GeneratorType.Query;
    LogicalOptimizerManager optimizerManager = LogicalOptimizerManager.getInstance();
    String[] optimizers = config.getQueryOptimizer().split(",");
    for (String optimizer : optimizers) {
      registerOptimizer(optimizerManager.getOptimizer(optimizer));
    }
  }

  public static QueryGenerator getInstance() {
    return instance;
  }

  @Override
  protected Operator generateRoot(Statement statement) {
    SelectStatement selectStatement = (SelectStatement) statement;
    // 计算cte的操作符树
    selectStatement
        .getCteList()
        .forEach(
            cte -> {
              Operator root = generateRoot(cte.getStatement());
              root = new Rename(new OperatorSource(root), cte.getAliasMap());
              cte.setRoot(root);
            });
    return generateRoot(selectStatement);
  }

  private Operator generateRoot(SelectStatement selectStatement) {
    if (selectStatement.getSelectType() == SelectStatement.SelectStatementType.UNARY) {
      return generateRoot((UnarySelectStatement) selectStatement);
    } else if (selectStatement.getSelectType() == SelectStatement.SelectStatementType.BINARY) {
      return generateRoot((BinarySelectStatement) selectStatement);
    } else {
      throw new RuntimeException(
          "Unknown select statement type: " + selectStatement.getSelectType());
    }
  }

  /**
   * 根据BinarySelectStatement，生成一个操作符树
   *
   * @param selectStatement Select上下文
   * @return 生成的操作符树
   */
  /*
   大体步骤如下：
   1. 根据Set Operator Type，初始化一个Union或Except或Intersect操作符及子树
   2. 如果有Order By子句或Limit子句，构建相关操作符
  */
  private Operator generateRoot(BinarySelectStatement selectStatement) {
    Operator root;

    checkIsSetOperator(selectStatement);

    root = initUnion(selectStatement);
    root = root != null ? root : initExcept(selectStatement);
    root = root != null ? root : initIntersect(selectStatement);

    root = buildOrderByPaths(selectStatement, root);

    root = buildLimit(selectStatement, root);

    return root;
  }

  /**
   * 根据UnarySelectStatement，生成一个操作符树
   *
   * @param selectStatement Select上下文
   * @return 生成的操作符树
   */
  /*
  大体步骤如下：
  1.首先根据SelectStatement的Project、Join、From部分，初始化一个操作符树，最终只会调用其中一个init函数
  2.检查操作符树是否为空，或者metaManager是否有可写的存储引擎，或者SubQuery是否有freeVariables
  3.按顺序处理其他相关操作符，并加入到操作符树中

  今后拓展generator时，应该该按照通过添加相应函数来拓展，而不是直接把逻辑写在该函数中
  1.添加初始化逻辑时，应该实现一个init函数，如果满足条件则返回一个操作符树，否则返回null。
  2.添加针对某个操作符的构建逻辑时，应该写一个build函数，如果满足条件则在root之上构建一个操作符，否则返回原root。
  3.添加检查逻辑时，应该写一个check函数，如果根据条件返回true/false,或者不满足条件时抛出异常。
  */
  private Operator generateRoot(UnarySelectStatement selectStatement) {
    Operator root;

    root = initProjectWaitingForPath(selectStatement);
    root = root != null ? root : initFilterAndMergeFragmentsWithJoin(selectStatement);
    root = root != null ? root : initFromPart(selectStatement);
    root = root != null ? root : initFilterAndMergeFragments(selectStatement);

    if (!checkRoot(root) && !checkIsMetaWritable()) {
      return null;
    }
    checkSubQueryHasFreeVariables(selectStatement);

    root = buildWhereSubQuery(selectStatement, root);

    root = buildValueFilter(selectStatement, root);

    root = buildSelectSubQuery(selectStatement, root);

    root = buildGroupByQuery(selectStatement, root);

    root = buildDownSampleQuery(selectStatement, root);

    root = buildAggregateQuery(selectStatement, root);

    root = buildLastFirstQuery(selectStatement, root);

    root = buildSimpleQuery(selectStatement, root);

    root = buildHavingSubQuery(selectStatement, root);

    root = buildHavingFilter(selectStatement, root);

    root = buildRowTransform(selectStatement, root);

    root = buildDistinct(selectStatement, root);

    root = buildOrderByPaths(selectStatement, root);

    root = buildLimit(selectStatement, root);

    root = buildReorder(selectStatement, root);

    root = buildRename(selectStatement, root);

    root = buildValueToMeta(selectStatement, root);

    return root;
  }

  /**
   * 如果selectStatement是SubQuery,检查它的freeVariables是否为空，为空则抛出异常
   *
   * @param selectStatement Select上下文
   */
  private static void checkSubQueryHasFreeVariables(UnarySelectStatement selectStatement) {
    if (selectStatement.isSubQuery()) {
      return;
    }
    selectStatement.initFreeVariables();
    List<String> freeVariables = selectStatement.getFreeVariables();
    if (!freeVariables.isEmpty()) {
      throw new RuntimeException("Unexpected paths' name: " + freeVariables + ".");
    }
  }

  /**
   * 如果root为空或者metaManager没有可写的存储引擎，返回false
   *
   * @param root 根节点
   * @return 如果root为空或者metaManager没有可写的存储引擎，返回false
   */
  private boolean checkRoot(Operator root) {
    return root != null;
  }

  private boolean checkIsMetaWritable() {
    return metaManager.hasWritableStorageEngines();
  }

  /**
   * 检查BinarySelectStatement的Set Operator Type是否是Union, Except, Intersect, 不是则抛出异常
   *
   * @param selectStatement Select上下文
   */
  private static void checkIsSetOperator(BinarySelectStatement selectStatement) {
    if (!OperatorType.isSetOperator(selectStatement.getSetOperator())) {
      throw new RuntimeException("Unknown set operator type: " + selectStatement.getSetOperator());
    }
  }

  /**
   * 获取SelectStatement的Column列表
   *
   * @param selectStatement Select上下文
   * @return Column列表
   */
  private List<String> getOrderList(SelectStatement selectStatement) {
    List<String> orderList = new ArrayList<>();
    selectStatement
        .getExpressions()
        .forEach(
            expression -> {
              String order =
                  expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
              orderList.add(order);
            });

    return orderList;
  }

  /**
   * 如果BinarySelectStatement的Set Operator Type是Union, 根据它的左右子查询，初始化一个Union操作符及子树
   *
   * @param selectStatement Select上下文
   * @return Union操作符及子树；如果Set Operator Type不是Union，返回null
   */
  private Operator initUnion(BinarySelectStatement selectStatement) {
    if (selectStatement.getSetOperator() != OperatorType.Union) {
      return null;
    }

    SelectStatement leftQuery = selectStatement.getLeftQuery();
    SelectStatement rightQuery = selectStatement.getRightQuery();

    return new Union(
        new OperatorSource(generateRoot(leftQuery)),
        new OperatorSource(generateRoot(rightQuery)),
        getOrderList(leftQuery),
        getOrderList(rightQuery),
        selectStatement.isDistinct());
  }
  /**
   * 如果BinarySelectStatement的Set Operator Type是Except, 根据它的左右子查询，初始化一个Except操作符及子树
   *
   * @param selectStatement Select上下文
   * @return Except操作符及子树；如果Set Operator Type不是Except，返回null
   */
  private Operator initExcept(BinarySelectStatement selectStatement) {
    if (selectStatement.getSetOperator() != OperatorType.Except) {
      return null;
    }
    SelectStatement leftQuery = selectStatement.getLeftQuery();
    SelectStatement rightQuery = selectStatement.getRightQuery();

    return new Except(
        new OperatorSource(generateRoot(leftQuery)),
        new OperatorSource(generateRoot(rightQuery)),
        getOrderList(leftQuery),
        getOrderList(rightQuery),
        selectStatement.isDistinct());
  }

  /**
   * 如果BinarySelectStatement的Set Operator Type是Intersect, 根据它的左右子查询，初始化一个Intersect操作符及子树
   *
   * @param selectStatement Select上下文
   * @return Intersect操作符及子树；如果Set Operator Type不是Intersect，返回null
   */
  private Operator initIntersect(BinarySelectStatement selectStatement) {
    if (selectStatement.getSetOperator() != OperatorType.Intersect) {
      return null;
    }
    SelectStatement leftQuery = selectStatement.getLeftQuery();
    SelectStatement rightQuery = selectStatement.getRightQuery();

    return new Intersect(
        new OperatorSource(generateRoot(leftQuery)),
        new OperatorSource(generateRoot(rightQuery)),
        getOrderList(leftQuery),
        getOrderList(rightQuery),
        selectStatement.isDistinct());
  }

  /**
   * 如果SelectStatement的from部分不为空，从from部分初始化一个操作符树
   *
   * @param selectStatement select语句上下文
   * @return from部分的操作符树；如果from部分为空，返回null
   */
  private Operator initFromPart(UnarySelectStatement selectStatement) {
    if (selectStatement.getFromParts().isEmpty()) {
      return null;
    }

    Operator root;
    FromPart fromPart = selectStatement.getFromPart(0);
    switch (fromPart.getType()) {
      case Path:
        policy.notify(selectStatement);
        root = filterAndMergeFragments(selectStatement);
        break;
      case SubQuery:
        SubQueryFromPart subQueryFromPart = (SubQueryFromPart) fromPart;
        root = generateRoot(subQueryFromPart.getSubQuery());
        break;
      case Cte:
        CteFromPart cteFromPart = (CteFromPart) fromPart;
        root = cteFromPart.getRoot().copy();
        break;
      case ShowColumns:
        ShowColumnsFromPart showColumnsFromPart = (ShowColumnsFromPart) fromPart;
        root = new ShowColumns(new GlobalSource(), showColumnsFromPart.getShowColumnsStatement());
        break;
      default:
        throw new RuntimeException("Unknown FromPart type: " + fromPart.getType());
    }
    if (fromPart.hasAlias()) {
      root = new Rename(new OperatorSource(root), fromPart.getAliasMap());
    }
    return root;
  }

  /**
   * 如果SelectStatement有Join部分，根据Tag Filter和Path Prefix过滤选择Fragments,并将它们Join成一个操作符树
   *
   * @param selectStatement Select上下文
   * @return Join操作符树；如果没有Join部分，返回null
   */
  private Operator initFilterAndMergeFragmentsWithJoin(UnarySelectStatement selectStatement) {
    if (!selectStatement.hasJoinParts()) {
      return null;
    }
    TagFilter tagFilter = selectStatement.getTagFilter();

    List<Operator> joinList = new ArrayList<>();
    // 1. get all data of single prefix like a.* or b.*
    selectStatement
        .getFromParts()
        .forEach(
            fromPart -> {
              Operator root;
              switch (fromPart.getType()) {
                case Path:
                  PathFromPart pathFromPart = (PathFromPart) fromPart;
                  String prefix = pathFromPart.getOriginPrefix() + ALL_PATH_SUFFIX;
                  Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
                      getFragmentsByColumnsInterval(
                          selectStatement, new ColumnsInterval(prefix, prefix));
                  Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
                  List<FragmentMeta> dummyFragments = pair.v;
                  root =
                      mergeRawData(
                          fragments, dummyFragments, Collections.singletonList(prefix), tagFilter);
                  break;
                case SubQuery:
                  SubQueryFromPart subQueryFromPart = (SubQueryFromPart) fromPart;
                  root = generateRoot(subQueryFromPart.getSubQuery());
                  break;
                case Cte:
                  CteFromPart cteFromPart = (CteFromPart) fromPart;
                  root = cteFromPart.getRoot().copy();
                  break;
                case ShowColumns:
                  ShowColumnsFromPart showColumnsFromPart = (ShowColumnsFromPart) fromPart;
                  root =
                      new ShowColumns(
                          new GlobalSource(), showColumnsFromPart.getShowColumnsStatement());
                  break;
                default:
                  throw new RuntimeException("Unknown FromPart type: " + fromPart.getType());
              }
              if (fromPart.hasAlias()) {
                root = new Rename(new OperatorSource(root), fromPart.getAliasMap());
              }
              joinList.add(root);
            });
    // 2. merge by declare
    Operator left = joinList.get(0);
    String prefixA = selectStatement.getFromPart(0).getPrefix();
    for (int i = 1; i < joinList.size(); i++) {
      JoinCondition joinCondition = selectStatement.getFromPart(i).getJoinCondition();
      Operator right = joinList.get(i);

      String prefixB = selectStatement.getFromPart(i).getPrefix();

      Filter filter = joinCondition.getFilter();
      List<String> joinColumns = joinCondition.getJoinColumns();
      boolean isNaturalJoin = isNaturalJoin(joinCondition.getJoinType());

      if (!joinColumns.isEmpty() || isNaturalJoin) {
        if (prefixA == null || prefixB == null) {
          throw new RuntimeException(
              "A natural join or a join with USING should have two public prefix");
        }
      }

      JoinAlgType joinAlgType = chooseJoinAlg(filter, isNaturalJoin, joinColumns);
      OuterJoinType outerJoinType = null;
      switch (joinCondition.getJoinType()) {
        case CrossJoin:
          left =
              new CrossJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB);
          break;
        case InnerJoin:
        case InnerNaturalJoin:
          left =
              new InnerJoin(
                  new OperatorSource(left),
                  new OperatorSource(right),
                  prefixA,
                  prefixB,
                  filter,
                  joinColumns,
                  isNaturalJoin,
                  joinAlgType);
          break;
        case LeftOuterJoin:
        case LeftNaturalJoin:
          outerJoinType = OuterJoinType.LEFT;
        case RightOuterJoin:
        case RightNaturalJoin:
          outerJoinType = outerJoinType == null ? OuterJoinType.RIGHT : outerJoinType;
        case FullOuterJoin:
          outerJoinType = outerJoinType == null ? OuterJoinType.FULL : outerJoinType;
          left =
              new OuterJoin(
                  new OperatorSource(left),
                  new OperatorSource(right),
                  prefixA,
                  prefixB,
                  outerJoinType,
                  filter,
                  joinColumns,
                  isNaturalJoin,
                  joinAlgType);
          break;
        default:
          break;
      }

      left =
          detectAndTranslateCorrelatedVariables(
              selectStatement.getFromPart(i).getFreeVariables(), selectStatement, i, left);

      prefixA = prefixB;
    }
    return left;
  }

  /**
   * 如果SelectStatement有ValueToSelectedPath，初始化生成一个ProjectWaitingForPath操作符
   *
   * @param selectStatement Select上下文
   * @return ProjectWaitingForPath操作符；如果没有ValueToSelectedPath，返回null
   */
  private static Operator initProjectWaitingForPath(UnarySelectStatement selectStatement) {
    if (selectStatement.hasValueToSelectedPath()) {
      return new ProjectWaitingForPath(selectStatement);
    }
    return null;
  }

  /**
   * 根据UnarySelectStatement，初始化一个根据Path过滤并合并Fragments的操作符树。
   * 该函数会在SelectStatement无From部分、无Join部分、无Project部分时被调用
   *
   * @param selectStatement Select上下文
   * @return 根据Path过滤并合并Fragments的操作符树
   */
  private Operator initFilterAndMergeFragments(UnarySelectStatement selectStatement) {
    policy.notify(selectStatement);
    return filterAndMergeFragments(selectStatement);
  }

  /**
   * 如果SelectStatement有LIMIT子句，在root之上构建一个Limit操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Limit操作符的根节点；如果没有LIMIT子句，返回原根节点
   */
  private static Operator buildLimit(SelectStatement selectStatement, Operator root) {
    if (selectStatement.getLimit() == Integer.MAX_VALUE && selectStatement.getOffset() == 0) {
      return root;
    }
    return new Limit(
        new OperatorSource(root),
        (int) selectStatement.getLimit(),
        (int) selectStatement.getOffset());
  }

  /**
   * 如果SelectStatement有Order By子句，在root之上构建一个Sort操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Sort操作符的根节点；如果没有Order By子句，返回原根节点
   */
  private static Operator buildOrderByPaths(SelectStatement selectStatement, Operator root) {
    if (selectStatement.getOrderByPaths().isEmpty()) {
      return root;
    }
    return new Sort(
        new OperatorSource(root),
        selectStatement.getOrderByPaths(),
        selectStatement.isAscending() ? Sort.SortType.ASC : Sort.SortType.DESC);
  }

  /**
   * 如果SelectStatement的QueryType是SimpleQuery，在root之上构建相关操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了相关操作符的根节点；如果QueryType不是SimpleQuery，返回原根节点
   */
  private static Operator buildSimpleQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getQueryType() != QueryType.SimpleQuery) {
      return root;
    }
    Set<String> selectedPath = new HashSet<>();
    selectStatement
        .getBaseExpressionList()
        .forEach(expression -> selectedPath.add(expression.getPathName()));

    return new Project(
        new OperatorSource(root),
        new ArrayList<>(selectedPath),
        selectStatement.getTagFilter(),
        selectStatement.hasValueToSelectedPath());
  }

  /**
   * 如果SelectStatement的QueryType是LastFirstQuery，在root之上构建相关操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了相关操作符的根节点；如果QueryType不是LastFirstQuery，返回原根节点
   */
  private static Operator buildLastFirstQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getQueryType() != QueryType.LastFirstQuery) {
      return root;
    }
    List<FunctionCall> functionCallList = new ArrayList<>();
    selectStatement
        .getFuncExpressionMap()
        .forEach(
            (k, v) ->
                v.forEach(
                    expression -> {
                      FunctionParams params = getFunctionParams(k, expression);
                      functionCallList.add(
                          new FunctionCall(functionManager.getFunction(k), params));
                    }));

    return new MappingTransform(new OperatorSource(root), functionCallList);
  }

  /**
   * 如果SelectStatement的QueryType是AggregateQuery，在root之上构建相关操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了相关操作符的根节点；如果QueryType不是AggregateQuery，返回原根节点
   */
  private static Operator buildAggregateQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getQueryType() != QueryType.AggregateQuery) {
      return root;
    }
    List<Operator> queryList = new ArrayList<>();
    List<FunctionCall> functionCallList = new ArrayList<>();
    Operator finalRoot = root;
    selectStatement
        .getFuncExpressionMap()
        .forEach(
            (k, v) ->
                v.forEach(
                    expression -> {
                      FunctionParams params = getFunctionParams(k, expression);
                      functionCallList.add(
                          new FunctionCall(functionManager.getFunction(k), params));
                    }));

    if (!functionCallList.isEmpty()) {
      switch (functionCallList.get(0).getFunction().getMappingType()) {
        case Mapping:
          queryList.add(new MappingTransform(new OperatorSource(finalRoot), functionCallList));
          break;
        case RowMapping:
          queryList.add(new RowTransform(new OperatorSource(finalRoot), functionCallList));
          break;
        case SetMapping:
          queryList.add(new SetTransform(new OperatorSource(finalRoot), functionCallList));
          break;
        default:
          throw new RuntimeException(
              "Unknown mapping type: " + functionCallList.get(0).getFunction().getMappingType());
      }
    }

    selectStatement
        .getBaseExpressionList()
        .forEach(
            expression -> {
              Operator copySelect = finalRoot.copy();
              queryList.add(
                  new Project(
                      new OperatorSource(copySelect),
                      Collections.singletonList(expression.getPathName()),
                      selectStatement.getTagFilter()));
            });

    if (selectStatement.getFuncTypeSet().contains(FuncType.Udtf)) {
      root = OperatorUtils.joinOperatorsByTime(queryList);
    } else {
      root = OperatorUtils.joinOperators(queryList, ORDINAL);
    }

    return root;
  }

  /**
   * 如果SelectStatement的QueryType是GroupByQuery，在root之上构建相关操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了相关操作符的根节点；如果QueryType不是GroupByQuery，返回原根节点
   */
  private Operator buildGroupByQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getQueryType() != QueryType.GroupByQuery) {
      return root;
    }
    List<FunctionCall> functionCallList = new ArrayList<>();
    selectStatement
        .getFuncExpressionMap()
        .forEach(
            (k, v) -> {
              if (!k.equals("")) {
                v.forEach(
                    expression -> {
                      FunctionParams params = getFunctionParams(k, expression);
                      functionCallList.add(
                          new FunctionCall(functionManager.getFunction(k), params));
                    });
              }
            });

    return new GroupBy(
        new OperatorSource(root), selectStatement.getGroupByPaths(), functionCallList);
  }

  /**
   * 如果SelectStatement的QueryType是DownSampleQuery，在root之上构建相关操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了相关操作符的根节点；如果QueryType不是DownSampleQuery，返回原根节点
   */
  private Operator buildDownSampleQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getQueryType() != QueryType.DownSampleQuery) {
      return root;
    }
    List<FunctionCall> functionCallList = new ArrayList<>();

    selectStatement
        .getFuncExpressionMap()
        .forEach(
            (k, v) ->
                v.forEach(
                    expression -> {
                      FunctionParams params = getFunctionParams(k, expression);
                      functionCallList.add(
                          new FunctionCall(functionManager.getFunction(k), params));
                    }));

    return new Downsample(
        new OperatorSource(root),
        selectStatement.getPrecision(),
        selectStatement.getSlideDistance(),
        functionCallList,
        new KeyRange(selectStatement.getStartKey(), selectStatement.getEndKey()));
  }

  /**
   * 根据SelectStatement构建查询树的Reorder操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Reorder操作符的根节点
   */
  private static Operator buildReorder(UnarySelectStatement selectStatement, Operator root) {
    boolean hasFuncWithArgs =
        selectStatement.getExpressions().stream()
            .anyMatch(
                expression -> {
                  if (!(expression instanceof FuncExpression)) {
                    return false;
                  }
                  FuncExpression funcExpression = ((FuncExpression) expression);
                  return !funcExpression.getArgs().isEmpty()
                      || !funcExpression.getKvargs().isEmpty();
                });

    if (selectStatement.getQueryType().equals(QueryType.LastFirstQuery)) {
      root = new Reorder(new OperatorSource(root), Arrays.asList("path", "value"));
    } else if (hasFuncWithArgs) {
      root = new Reorder(new OperatorSource(root), Collections.singletonList("*"));
    } else {
      List<String> order = new ArrayList<>();
      List<Boolean> isPyUDF = new ArrayList<>();
      selectStatement
          .getExpressions()
          .forEach(
              expression -> {
                if (expression.getType().equals(Expression.ExpressionType.FromValue)) {
                  return;
                }
                if (expression.getType().equals(Expression.ExpressionType.Function)) {
                  isPyUDF.add(((FuncExpression) expression).isPyUDF());
                } else {
                  isPyUDF.add(false);
                }
                String colName = expression.getColumnName();
                order.add(colName);
              });
      root =
          new Reorder(
              new OperatorSource(root), order, isPyUDF, selectStatement.hasValueToSelectedPath());
    }
    return root;
  }

  /**
   * 如果SelectStatement有AliasMap, 在root之上构建一个Rename操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Rename操作符的根节点；如果没有AliasMap，返回原根节点
   */
  private static Operator buildRename(UnarySelectStatement selectStatement, Operator root) {
    Map<String, String> aliasMap = selectStatement.getSelectAliasMap();
    if (!aliasMap.isEmpty()) {
      root = new Rename(new OperatorSource(root), aliasMap);
    }
    return root;
  }

  /**
   * 如果SelectStatement有ValueToSelectedPath, 在root之上构建一个ValueToSelectedPath操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了ValueToSelectedPath操作符的根节点；如果没有ValueToSelectedPath，返回原根节点
   */
  private Operator buildValueToMeta(UnarySelectStatement selectStatement, Operator root) {
    if (!selectStatement.hasValueToSelectedPath()) {
      return root;
    }
    List<Source> valueToMetaList = new ArrayList<>();
    selectStatement
        .getExpressions()
        .forEach(
            expression -> {
              if (expression.getType().equals(Expression.ExpressionType.FromValue)) {
                FromValueExpression fvExpression = (FromValueExpression) expression;
                Operator child = generateRoot(fvExpression.getSubStatement());

                String prefix = "";
                if (selectStatement.isFromSinglePath()) {
                  prefix = selectStatement.getFromPart(0).getOriginPrefix();
                }

                valueToMetaList.add(
                    new OperatorSource(new ValueToSelectedPath(new OperatorSource(child), prefix)));
              }
            });
    root = new FoldedOperator(valueToMetaList, root);

    return root;
  }

  /**
   * 如果SelectStatement有Distinct, 在root之上构建一个Distinct操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Distinct操作符的根节点；如果没有Distinct，返回原根节点
   */
  private static Operator buildDistinct(UnarySelectStatement selectStatement, Operator root) {
    if (!selectStatement.isDistinct()) {
      return root;
    }
    List<String> patterns = new ArrayList<>();
    for (Expression expression : selectStatement.getExpressions()) {
      patterns.add(expression.getColumnName());
    }
    root = new Distinct(new OperatorSource(root), patterns);

    return root;
  }

  /**
   * 如果SelectStatement有RowTransform, 在root之上构建一个RowTransform操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了RowTransform操作符的根节点；如果没有RowTransform，返回原根节点
   */
  private static Operator buildRowTransform(UnarySelectStatement selectStatement, Operator root) {
    if (!selectStatement.needRowTransform()) {
      return root;
    }
    List<FunctionCall> functionCallList = new ArrayList<>();
    for (Expression expression : selectStatement.getExpressions()) {
      FunctionParams params = new FunctionParams(expression);
      functionCallList.add(new FunctionCall(functionManager.getFunction(ARITHMETIC_EXPR), params));
    }
    root = new RowTransform(new OperatorSource(root), functionCallList);

    return root;
  }

  /**
   * 如果SelectStatement有HavingSubQuery, 以root为基础构建一个Join起来的树
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return Join起来的带Having子查询的树；如果没有HavingSubQuery，返回原根节点
   */
  private Operator buildHavingSubQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getHavingSubQueryParts().isEmpty()) {
      return root;
    }
    return buildJoinOperatorFromSubQueries(
        selectStatement, root, selectStatement.getHavingSubQueryParts());
  }

  /**
   * 如果SelectStatement有HavingFilter, 在root之上构建一个Select操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Select操作符的根节点；如果没有HavingFilter，返回原根节点
   */
  private Operator buildHavingFilter(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getHavingFilter() != null) {
      root = new Select(new OperatorSource(root), selectStatement.getHavingFilter(), null);
    }
    return root;
  }

  /**
   * 如果SelectStatement有SelectSubQuery, 以root为基础构建一个Join起来的树
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return Join起来的带Select子查询的树；如果没有SelectSubQuery，返回原根节点
   */
  private Operator buildSelectSubQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getSelectSubQueryParts().isEmpty()) {
      return root;
    }
    int sizeSelectSubQuery = selectStatement.getSelectSubQueryParts().size();
    List<SubQueryFromPart> selectSubQueryParts = selectStatement.getSelectSubQueryParts();
    for (int i = 0; i < sizeSelectSubQuery; i++) {
      if (selectSubQueryParts.get(i).getJoinCondition().getJoinType() == JoinType.SingleJoin) {
        Operator right = generateRoot(selectSubQueryParts.get(i).getSubQuery());

        Filter filter = selectSubQueryParts.get(i).getJoinCondition().getFilter();
        JoinAlgType joinAlgType = chooseJoinAlg(filter);

        root =
            new SingleJoin(
                new OperatorSource(root), new OperatorSource(right), filter, joinAlgType);
        root =
            detectAndTranslateCorrelatedVariables(
                selectSubQueryParts.get(i).getFreeVariables(),
                selectStatement,
                selectStatement.getFromParts().size(),
                root);
      }
    }

    return root;
  }

  /**
   * 如果SelectStatement有ValueFilter, 在root之上构建一个Select操作符
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return 添加了Select操作符的根节点；如果没有ValueFilter，返回原根节点
   */
  private static Operator buildValueFilter(UnarySelectStatement selectStatement, Operator root) {
    if (!selectStatement.hasValueFilter()) {
      return root;
    }
    return new Select(
        new OperatorSource(root), selectStatement.getFilter(), selectStatement.getTagFilter());
  }

  /**
   * 如果SelectStatement有WhereSubQuery, 以root为基础构建一个Join起来的树
   *
   * @param selectStatement Select上下文
   * @param root 当前根节点
   * @return Join起来的带Where子查询的树；如果没有WhereSubQuery，返回原根节点
   */
  private Operator buildWhereSubQuery(UnarySelectStatement selectStatement, Operator root) {
    if (selectStatement.getWhereSubQueryParts().isEmpty()) {
      return root;
    }
    return buildJoinOperatorFromSubQueries(
        selectStatement, root, selectStatement.getWhereSubQueryParts());
  }

  /** 从子查询构建一个Join起来的子树，用于处理where子查询和having子查询 */
  private Operator buildJoinOperatorFromSubQueries(
      UnarySelectStatement selectStatement, Operator root, List<SubQueryFromPart> subQueryParts) {
    for (SubQueryFromPart whereSubQueryPart : subQueryParts) {
      Operator right = generateRoot(whereSubQueryPart.getSubQuery());

      Filter filter = whereSubQueryPart.getJoinCondition().getFilter();
      String markColumn = whereSubQueryPart.getJoinCondition().getMarkColumn();
      boolean isAntiJoin = whereSubQueryPart.getJoinCondition().isAntiJoin();
      JoinAlgType joinAlgType = chooseJoinAlg(filter);

      if (whereSubQueryPart.getJoinCondition().getJoinType() == JoinType.MarkJoin) {
        root =
            new MarkJoin(
                new OperatorSource(root),
                new OperatorSource(right),
                filter,
                markColumn,
                isAntiJoin,
                joinAlgType);
      } else if (whereSubQueryPart.getJoinCondition().getJoinType() == JoinType.SingleJoin) {
        root =
            new SingleJoin(
                new OperatorSource(root), new OperatorSource(right), filter, joinAlgType);
      }
      root =
          detectAndTranslateCorrelatedVariables(
              whereSubQueryPart.getFreeVariables(),
              selectStatement,
              selectStatement.getFromParts().size(),
              root);
    }
    return root;
  }

  /** 根据自由变量判断是否需要将apply算子下推 */
  private static Operator detectAndTranslateCorrelatedVariables(
      List<String> freeVariables, UnarySelectStatement selectStatement, int index, Operator root) {
    List<String> correlatedVariables = new ArrayList<>();
    // 判断右子树中的自由变量是否来自左子树，如果是，记为关联变量
    for (String freeVariable : freeVariables) {
      if (selectStatement.hasAttribute(freeVariable, index)) {
        correlatedVariables.add(freeVariable);
      }
    }
    // 如果存在关联变量，则将apply算子下推
    if (!correlatedVariables.isEmpty()) {
      root = translateApply(root, correlatedVariables);
    }
    return root;
  }

  /** 根据Tag Filter和Project Path，过滤并合并Fragments，生成一棵树 */
  private Operator filterAndMergeFragments(UnarySelectStatement selectStatement) {
    List<String> pathList =
        SortUtils.mergeAndSortPaths(new ArrayList<>(selectStatement.getPathSet()));
    TagFilter tagFilter = selectStatement.getTagFilter();

    ColumnsInterval columnsInterval =
        new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));

    Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
        getFragmentsByColumnsInterval(selectStatement, columnsInterval);
    Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
    List<FragmentMeta> dummyFragments = pair.v;

    return mergeRawData(fragments, dummyFragments, pathList, tagFilter);
  }

  /** 从Expression中获取params */
  private static FunctionParams getFunctionParams(String functionName, FuncExpression expression) {
    return FunctionUtils.isCanUseSetQuantifierFunction(functionName)
        ? new FunctionParams(
            expression.getColumns(),
            expression.getArgs(),
            expression.getKvargs(),
            expression.isDistinct())
        : new FunctionParams(expression.getColumns(), expression.getArgs(), expression.getKvargs());
  }
}
