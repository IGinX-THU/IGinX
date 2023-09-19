package cn.edu.tsinghua.iginx.engine.logical.generator;

import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.getFragmentsByColumnsInterval;
import static cn.edu.tsinghua.iginx.engine.logical.utils.MetaUtils.mergeRawData;
import static cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils.translateApply;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.ORDINAL;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr.ARITHMETIC_EXPR;
import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.chooseJoinAlg;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;
import static cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType.isNaturalJoin;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.LogicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Distinct;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.engine.shared.operator.Except;
import cn.edu.tsinghua.iginx.engine.shared.operator.FoldedOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Intersect;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;
import cn.edu.tsinghua.iginx.engine.shared.operator.MappingTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.ProjectWaitingForPath;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.ValueToSelectedPath;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.FromValueExpression;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.PathFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.BinarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement.QueryType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jdk.nashorn.internal.runtime.regexp.joni.exception.SyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryGenerator extends AbstractGenerator {

  private static final Logger logger = LoggerFactory.getLogger(QueryGenerator.class);
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
    if (selectStatement.getSelectType() == SelectStatement.SelectStatementType.UNARY) {
      return generateRoot((UnarySelectStatement) selectStatement);
    } else if (selectStatement.getSelectType() == SelectStatement.SelectStatementType.BINARY) {
      return generateRoot((BinarySelectStatement) selectStatement);
    } else {
      throw new RuntimeException(
          "Unknown select statement type: " + selectStatement.getSelectType());
    }
  }

  private Operator generateRoot(BinarySelectStatement selectStatement) {
    Operator root;
    Operator left = generateRoot(selectStatement.getLeftQuery());
    Operator right = generateRoot(selectStatement.getRightQuery());

    List<String> leftOrder = new ArrayList<>();
    selectStatement
        .getLeftQuery()
        .getExpressions()
        .forEach(
            expression -> {
              String order =
                  expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
              leftOrder.add(order);
            });

    List<String> rightOrder = new ArrayList<>();
    selectStatement
        .getRightQuery()
        .getExpressions()
        .forEach(
            expression -> {
              String order =
                  expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
              rightOrder.add(order);
            });

    switch (selectStatement.getSetOperator()) {
      case Union:
        root =
            new Union(
                new OperatorSource(left),
                new OperatorSource(right),
                leftOrder,
                rightOrder,
                selectStatement.isDistinct());
        break;
      case Except:
        root =
            new Except(
                new OperatorSource(left),
                new OperatorSource(right),
                leftOrder,
                rightOrder,
                selectStatement.isDistinct());
        break;
      case Intersect:
        root =
            new Intersect(
                new OperatorSource(left),
                new OperatorSource(right),
                leftOrder,
                rightOrder,
                selectStatement.isDistinct());
        break;
      default:
        throw new RuntimeException(
            "Unknown set operator type: " + selectStatement.getSetOperator());
    }

    if (!selectStatement.getOrderByPaths().isEmpty()) {
      root =
          new Sort(
              new OperatorSource(root),
              selectStatement.getOrderByPaths(),
              selectStatement.isAscending() ? Sort.SortType.ASC : Sort.SortType.DESC);
    }

    if (selectStatement.getLimit() != Integer.MAX_VALUE || selectStatement.getOffset() != 0) {
      root =
          new Limit(
              new OperatorSource(root),
              (int) selectStatement.getLimit(),
              (int) selectStatement.getOffset());
    }

    return root;
  }

  private Operator generateRoot(UnarySelectStatement selectStatement) {
    Operator root;
    if (selectStatement.hasValueToSelectedPath()) {
      root = new ProjectWaitingForPath(selectStatement);
    } else if (selectStatement.hasJoinParts()) {
      root = filterAndMergeFragmentsWithJoin(selectStatement);
    } else if (!selectStatement.getFromParts().isEmpty()
        && selectStatement.getFromParts().get(0).getType() == FromPartType.SubQueryFromPart) {
      SubQueryFromPart fromPart = (SubQueryFromPart) selectStatement.getFromParts().get(0);
      root = generateRoot(fromPart.getSubQuery());
      if (fromPart.hasAlias()) {
        Map<String, String> map = fromPart.getSubQuery().getSubQueryAliasMap(fromPart.getAlias());
        root = new Rename(new OperatorSource(root), map);
      }
    } else {
      policy.notify(selectStatement);
      root = filterAndMergeFragments(selectStatement);
      if (!selectStatement.getFromParts().isEmpty()
          && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
        PathFromPart pathFromPart = (PathFromPart) selectStatement.getFromParts().get(0);
        if (pathFromPart.hasAlias()) {
          Map<String, String> map =
              selectStatement.getFromPathAliasMap(
                  pathFromPart.getOriginPrefix(), pathFromPart.getAlias());
          root = new Rename(new OperatorSource(root), map);
        }
      }
    }

    if (root == null && !metaManager.hasWritableStorageEngines()) {
      return null;
    }

    // 处理where子查询
    if (!selectStatement.getWhereSubQueryParts().isEmpty()) {
      int sizeWhereSubQueryParts = selectStatement.getWhereSubQueryParts().size();
      List<SubQueryFromPart> whereSubQueryParts = selectStatement.getWhereSubQueryParts();
      for (int i = 0; i < sizeWhereSubQueryParts; i++) {
        SubQueryFromPart whereSubQueryPart = whereSubQueryParts.get(i);
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
        List<String> freeVariables = whereSubQueryParts.get(i).getFreeVariables();
        List<String> correlatedVariables = new ArrayList<>();
        // 判断右子树中的自由变量是否来自左子树，如果是，记为关联变量
        for (String freeVariable : freeVariables) {
          if (selectStatement.hasAttribute(freeVariable, selectStatement.getFromParts().size())) {
            correlatedVariables.add(freeVariable);
          }
        }
        // 如果右子树中存在关联变量，则将apply算子下推
        if (!correlatedVariables.isEmpty()) {
          root = translateApply(root, correlatedVariables);
        }
      }
    }

    TagFilter tagFilter = selectStatement.getTagFilter();

    if (selectStatement.hasValueFilter()) {
      root = new Select(new OperatorSource(root), selectStatement.getFilter(), tagFilter);
    }

    // 处理select子查询
    if (!selectStatement.getSelectSubQueryParts().isEmpty()) {
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
          List<String> freeVariables = selectSubQueryParts.get(i).getFreeVariables();
          List<String> correlatedVariables = new ArrayList<>();
          // 判断右子树中的自由变量是否来自左子树，如果是，记为关联变量
          for (String freeVariable : freeVariables) {
            if (selectStatement.hasAttribute(freeVariable, selectStatement.getFromParts().size())) {
              correlatedVariables.add(freeVariable);
            }
          }
          // 如果存在关联变量，则将apply算子下推
          if (!correlatedVariables.isEmpty()) {
            root = translateApply(root, correlatedVariables);
          }
        }
      }
    }

    List<Operator> queryList = new ArrayList<>();
    if (selectStatement.getQueryType() == QueryType.GroupByQuery) {
      // GroupBy Query
      List<FunctionCall> functionCallList = new ArrayList<>();
      selectStatement
          .getFuncExpressionMap()
          .forEach(
              (k, v) -> {
                if (!k.equals("")) {
                  v.forEach(
                      expression -> {
                        FunctionParams params =
                            FunctionUtils.isCanUseSetQuantifierFunction(k)
                                ? new FunctionParams(
                                    expression.getParams(), expression.isDistinct())
                                : new FunctionParams(expression.getParams());

                        functionCallList.add(
                            new FunctionCall(functionManager.getFunction(k), params));
                      });
                }
              });
      queryList.add(
          new GroupBy(
              new OperatorSource(root), selectStatement.getGroupByPaths(), functionCallList));
    } else if (selectStatement.getQueryType() == QueryType.DownSampleQuery) {
      // DownSample Query
      Operator finalRoot = root;
      selectStatement
          .getFuncExpressionMap()
          .forEach(
              (k, v) ->
                  v.forEach(
                      expression -> {
                        List<Integer> levels =
                            selectStatement.getLayers().isEmpty()
                                ? null
                                : selectStatement.getLayers();

                        FunctionParams params =
                            FunctionUtils.isCanUseSetQuantifierFunction(k)
                                ? new FunctionParams(
                                    expression.getParams(), levels, expression.isDistinct())
                                : new FunctionParams(expression.getParams(), levels);

                        Operator copySelect = finalRoot.copy();
                        queryList.add(
                            new Downsample(
                                new OperatorSource(copySelect),
                                selectStatement.getPrecision(),
                                selectStatement.getSlideDistance(),
                                new FunctionCall(functionManager.getFunction(k), params),
                                new KeyRange(
                                    selectStatement.getStartKey(), selectStatement.getEndKey())));
                      }));
    } else if (selectStatement.getQueryType() == QueryType.AggregateQuery) {
      // Aggregate Query
      Operator finalRoot = root;
      selectStatement
          .getFuncExpressionMap()
          .forEach(
              (k, v) ->
                  v.forEach(
                      expression -> {
                        List<Integer> levels =
                            selectStatement.getLayers().isEmpty()
                                ? null
                                : selectStatement.getLayers();

                        FunctionParams params =
                            FunctionUtils.isCanUseSetQuantifierFunction(k)
                                ? new FunctionParams(
                                    expression.getParams(), levels, expression.isDistinct())
                                : new FunctionParams(expression.getParams(), levels);

                        Operator copySelect = finalRoot.copy();
                        logger.info("function: " + expression.getColumnName());
                        if (FunctionUtils.isRowToRowFunction(k)) {
                          queryList.add(
                              new RowTransform(
                                  new OperatorSource(copySelect),
                                  new FunctionCall(functionManager.getFunction(k), params)));
                        } else if (FunctionUtils.isSetToSetFunction(k)) {
                          queryList.add(
                              new MappingTransform(
                                  new OperatorSource(copySelect),
                                  new FunctionCall(functionManager.getFunction(k), params)));
                        } else {
                          queryList.add(
                              new SetTransform(
                                  new OperatorSource(copySelect),
                                  new FunctionCall(functionManager.getFunction(k), params)));
                        }
                      }));
      selectStatement
          .getBaseExpressionList()
          .forEach(
              expression -> {
                Operator copySelect = finalRoot.copy();
                queryList.add(
                    new Project(
                        new OperatorSource(copySelect),
                        Collections.singletonList(expression.getPathName()),
                        tagFilter));
              });
    } else if (selectStatement.getQueryType() == QueryType.LastFirstQuery) {
      Operator finalRoot = root;
      selectStatement
          .getFuncExpressionMap()
          .forEach(
              (k, v) ->
                  v.forEach(
                      expression -> {
                        FunctionParams params = new FunctionParams(expression.getParams());
                        Operator copySelect = finalRoot.copy();
                        logger.info("function: " + k + ", wrapped path: " + v);
                        queryList.add(
                            new MappingTransform(
                                new OperatorSource(copySelect),
                                new FunctionCall(functionManager.getFunction(k), params)));
                      }));
    } else {
      Set<String> selectedPath = new HashSet<>();
      selectStatement
          .getBaseExpressionList()
          .forEach(expression -> selectedPath.add(expression.getPathName()));
      queryList.add(
          new Project(
              new OperatorSource(root),
              new ArrayList<>(selectedPath),
              tagFilter,
              selectStatement.hasValueToSelectedPath()));
    }

    if (selectStatement.getQueryType() == QueryType.LastFirstQuery) {
      root = OperatorUtils.unionOperators(queryList);
    } else if (selectStatement.getQueryType() == QueryType.DownSampleQuery) {
      root = OperatorUtils.joinOperatorsByTime(queryList);
    } else {
      if (selectStatement.getFuncTypeSet().contains(FuncType.Udtf)) {
        root = OperatorUtils.joinOperatorsByTime(queryList);
      } else {
        root = OperatorUtils.joinOperators(queryList, ORDINAL);
      }
    }

    // 处理having子查询
    if (!selectStatement.getHavingSubQueryParts().isEmpty()) {
      int sizeHavingSubQueryParts = selectStatement.getHavingSubQueryParts().size();
      List<SubQueryFromPart> havingSubQueryParts = selectStatement.getHavingSubQueryParts();
      for (int i = 0; i < sizeHavingSubQueryParts; i++) {
        SubQueryFromPart havingSubQueryPart = havingSubQueryParts.get(i);
        Operator right = generateRoot(havingSubQueryPart.getSubQuery());

        Filter filter = havingSubQueryPart.getJoinCondition().getFilter();
        String markColumn = havingSubQueryPart.getJoinCondition().getMarkColumn();
        boolean isAntiJoin = havingSubQueryPart.getJoinCondition().isAntiJoin();
        JoinAlgType joinAlgType = chooseJoinAlg(filter);

        if (havingSubQueryPart.getJoinCondition().getJoinType() == JoinType.MarkJoin) {
          root =
              new MarkJoin(
                  new OperatorSource(root),
                  new OperatorSource(right),
                  filter,
                  markColumn,
                  isAntiJoin,
                  joinAlgType);
        } else if (havingSubQueryPart.getJoinCondition().getJoinType() == JoinType.SingleJoin) {
          root =
              new SingleJoin(
                  new OperatorSource(root), new OperatorSource(right), filter, joinAlgType);
        }
        List<String> freeVariables = havingSubQueryParts.get(i).getFreeVariables();
        List<String> correlatedVariables = new ArrayList<>();
        // 判断右子树中的自由变量是否来自左子树，如果是，记为关联变量
        for (String freeVariable : freeVariables) {
          if (selectStatement.hasAttribute(freeVariable, selectStatement.getFromParts().size())) {
            correlatedVariables.add(freeVariable);
          }
        }
        // 如果右子树中存在关联变量，则将apply算子下推
        if (!correlatedVariables.isEmpty()) {
          root = translateApply(root, correlatedVariables);
        }
      }
    }

    if (selectStatement.getHavingFilter() != null) {
      root = new Select(new OperatorSource(root), selectStatement.getHavingFilter(), null);
    }

    if (selectStatement.needRowTransform()) {
      List<FunctionCall> functionCallList = new ArrayList<>();
      for (Expression expression : selectStatement.getExpressions()) {
        FunctionParams params = new FunctionParams(expression);
        functionCallList.add(
            new FunctionCall(functionManager.getFunction(ARITHMETIC_EXPR), params));
      }
      root = new RowTransform(new OperatorSource(root), functionCallList);
    }

    if (selectStatement.isDistinct()) {
      root = new Distinct(new OperatorSource(root));
    }

    if (!selectStatement.isSubQuery()) {
      selectStatement.initFreeVariables();
      List<String> freeVariables = selectStatement.getFreeVariables();
      if (!freeVariables.isEmpty()) {
        throw new SyntaxException("Unexpected paths' name: " + freeVariables + ".");
      }
    }

    if (!selectStatement.getOrderByPaths().isEmpty()) {
      root =
          new Sort(
              new OperatorSource(root),
              selectStatement.getOrderByPaths(),
              selectStatement.isAscending() ? Sort.SortType.ASC : Sort.SortType.DESC);
    }

    if (selectStatement.getLimit() != Integer.MAX_VALUE || selectStatement.getOffset() != 0) {
      root =
          new Limit(
              new OperatorSource(root),
              (int) selectStatement.getLimit(),
              (int) selectStatement.getOffset());
    }

    // 子查询不生成Reorder算子
    if (!selectStatement.isSubQuery()) {
      if (selectStatement.getLayers().isEmpty()) {
        if (selectStatement.getQueryType().equals(QueryType.LastFirstQuery)) {
          root = new Reorder(new OperatorSource(root), Arrays.asList("path", "value"));
        } else {
          List<String> order = new ArrayList<>();
          selectStatement
              .getExpressions()
              .forEach(
                  expression -> {
                    if (expression.getType().equals(Expression.ExpressionType.FromValue)) {
                      return;
                    }
                    String colName = expression.getColumnName();
                    order.add(colName);
                  });
          root =
              new Reorder(
                  new OperatorSource(root), order, selectStatement.hasValueToSelectedPath());
        }
      } else {
        List<String> order = new ArrayList<>();
        selectStatement
            .getExpressions()
            .forEach(
                expression -> {
                  String colName = expression.getColumnName();
                  colName =
                      colName.replaceFirst(
                          selectStatement.getFromParts().get(0).getPrefix() + DOT, "");
                  order.add(colName);
                });
        root = new Reorder(new OperatorSource(root), order);
      }
    }

    Map<String, String> aliasMap = selectStatement.getSelectAliasMap();
    if (!aliasMap.isEmpty()) {
      root = new Rename(new OperatorSource(root), aliasMap);
    }

    List<Source> valueToMetaList = new ArrayList<>();
    if (selectStatement.hasValueToSelectedPath()) {
      selectStatement
          .getExpressions()
          .forEach(
              expression -> {
                if (expression.getType().equals(Expression.ExpressionType.FromValue)) {
                  FromValueExpression fvExpression = (FromValueExpression) expression;
                  Operator child = generateRoot(fvExpression.getSubStatement());

                  String prefix = "";
                  if (selectStatement.getFromParts().size() == 1
                      && selectStatement.getFromParts().get(0).getType()
                          == FromPartType.PathFromPart) {
                    prefix =
                        ((PathFromPart) selectStatement.getFromParts().get(0)).getOriginPrefix();
                  }

                  valueToMetaList.add(
                      new OperatorSource(
                          new ValueToSelectedPath(new OperatorSource(child), prefix)));
                }
              });
      root = new FoldedOperator(valueToMetaList, root);
    }

    return root;
  }

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

  private Operator filterAndMergeFragmentsWithJoin(UnarySelectStatement selectStatement) {
    TagFilter tagFilter = selectStatement.getTagFilter();

    List<Operator> joinList = new ArrayList<>();
    // 1. get all data of single prefix like a.* or b.*
    selectStatement
        .getFromParts()
        .forEach(
            fromPart -> {
              if (fromPart.getType() == FromPartType.SubQueryFromPart) {
                SubQueryFromPart subQueryFromPart = (SubQueryFromPart) fromPart;
                Operator root = generateRoot(subQueryFromPart.getSubQuery());
                // 子查询重命名
                if (subQueryFromPart.hasAlias()) {
                  Map<String, String> map =
                      subQueryFromPart
                          .getSubQuery()
                          .getSubQueryAliasMap(subQueryFromPart.getAlias());
                  root = new Rename(new OperatorSource(root), map);
                }
                joinList.add(root);
              } else {
                PathFromPart pathFromPart = (PathFromPart) fromPart;
                String prefix = pathFromPart.getOriginPrefix() + ALL_PATH_SUFFIX;
                Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
                    getFragmentsByColumnsInterval(
                        selectStatement, new ColumnsInterval(prefix, prefix));
                Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
                List<FragmentMeta> dummyFragments = pair.v;
                Operator root =
                    mergeRawData(
                        fragments, dummyFragments, Collections.singletonList(prefix), tagFilter);
                // from的序列的前缀重命名
                if (pathFromPart.hasAlias()) {
                  Map<String, String> map =
                      selectStatement.getFromPathAliasMap(
                          pathFromPart.getOriginPrefix(), pathFromPart.getAlias());
                  root = new Rename(new OperatorSource(root), map);
                }
                joinList.add(root);
              }
            });
    // 2. merge by declare
    Operator left = joinList.get(0);
    String prefixA = selectStatement.getFromParts().get(0).getPrefix();
    for (int i = 1; i < joinList.size(); i++) {
      JoinCondition joinCondition = selectStatement.getFromParts().get(i).getJoinCondition();
      Operator right = joinList.get(i);

      String prefixB = selectStatement.getFromParts().get(i).getPrefix();

      Filter filter = joinCondition.getFilter();
      List<String> joinColumns = joinCondition.getJoinColumns();
      boolean isNaturalJoin = isNaturalJoin(joinCondition.getJoinType());
      if (joinColumns == null) {
        joinColumns = new ArrayList<>();
      }

      if (!joinColumns.isEmpty() || isNaturalJoin) {
        if (prefixA == null || prefixB == null) {
          throw new RuntimeException(
              "A natural join or a join with USING should have two public prefix");
        }
      }

      JoinAlgType joinAlgType = chooseJoinAlg(filter, isNaturalJoin, joinColumns);

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
          left =
              new OuterJoin(
                  new OperatorSource(left),
                  new OperatorSource(right),
                  prefixA,
                  prefixB,
                  OuterJoinType.LEFT,
                  filter,
                  joinColumns,
                  isNaturalJoin,
                  joinAlgType);
          break;
        case RightOuterJoin:
        case RightNaturalJoin:
          left =
              new OuterJoin(
                  new OperatorSource(left),
                  new OperatorSource(right),
                  prefixA,
                  prefixB,
                  OuterJoinType.RIGHT,
                  filter,
                  joinColumns,
                  isNaturalJoin,
                  joinAlgType);
          break;
        case FullOuterJoin:
          left =
              new OuterJoin(
                  new OperatorSource(left),
                  new OperatorSource(right),
                  prefixA,
                  prefixB,
                  OuterJoinType.FULL,
                  filter,
                  joinColumns,
                  isNaturalJoin,
                  joinAlgType);
          break;
        default:
          break;
      }

      List<String> freeVariables = selectStatement.getFromParts().get(i).getFreeVariables();
      List<String> correlatedVariables = new ArrayList<>();
      // 判断右子树中的自由变量是否来自左子树，如果是，记为关联变量
      for (String freeVariable : freeVariables) {
        if (selectStatement.hasAttribute(freeVariable, i)) {
          correlatedVariables.add(freeVariable);
        }
      }
      // 如果右子树中存在关联变量，则将apply算子下推
      if (!correlatedVariables.isEmpty()) {
        left = translateApply(left, correlatedVariables);
      }

      prefixA = prefixB;
    }
    return left;
  }
}
