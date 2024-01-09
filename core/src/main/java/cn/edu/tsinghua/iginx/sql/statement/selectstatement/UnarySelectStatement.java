package cn.edu.tsinghua.iginx.sql.statement.selectstatement;

import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.L_PARENTHESES;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.R_PARENTHESES;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.seslectstatementclause.*;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnarySelectStatement extends SelectStatement {
  private QueryType queryType;

  private boolean hasFunc;

  private final List<Expression> expressions;
  private final List<BaseExpression> baseExpressionList;
  private final Map<String, List<FuncExpression>> funcExpressionMap;
  private final Set<FuncType> funcTypeSet;
  private final Set<String> pathSet;

  private final SelectClause selectClause;
  private final FromClause fromClause;
  private final WhereClause whereClause;
  private final GroupByClause groupByClause;
  private final HavingClause havingClause;

  private long precision;
  private long startKey;
  private long endKey;
  private long slideDistance;

  public UnarySelectStatement() {
    this(false);
  }

  public UnarySelectStatement(boolean isSubQuery) {
    super(isSubQuery);
    this.selectStatementType = SelectStatementType.UNARY;
    this.queryType = QueryType.Unknown;
    this.expressions = new ArrayList<>();
    this.funcExpressionMap = new HashMap<>();
    this.baseExpressionList = new ArrayList<>();
    this.funcTypeSet = new HashSet<>();
    this.pathSet = new HashSet<>();

    this.selectClause = new SelectClause();
    this.fromClause = new FromClause();
    this.whereClause = new WhereClause();
    this.groupByClause = new GroupByClause();
    this.havingClause = new HavingClause();
  }

  // simple query
  public UnarySelectStatement(List<String> paths, long startKey, long endKey) {
    this(false);

    this.queryType = QueryType.SimpleQuery;

    paths.forEach(
        path -> {
          BaseExpression baseExpression = new BaseExpression(path);
          expressions.add(baseExpression);
          setSelectedPaths(baseExpression);
        });
    this.hasFunc = false;

    this.setFromSession(startKey, endKey);
  }

  // aggregate query
  public UnarySelectStatement(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType) {
    this(false);

    if (aggregateType == AggregateType.LAST || aggregateType == AggregateType.FIRST) {
      this.queryType = QueryType.LastFirstQuery;
    } else {
      this.queryType = QueryType.AggregateQuery;
    }

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          FuncExpression funcExpression = new FuncExpression(func, Collections.singletonList(path));
          expressions.add(funcExpression);
          setSelectedFuncsAndExpression(func, funcExpression);
        });

    this.hasFunc = true;

    this.setFromSession(startKey, endKey);
  }

  // downSample query
  public UnarySelectStatement(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType, long precision) {
    this(paths, startKey, endKey, aggregateType, precision, precision);
  }

  // downsample with slide window query
  public UnarySelectStatement(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      long precision,
      long slideDistance) {
    this(false);
    this.queryType = QueryType.DownSampleQuery;

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          FuncExpression funcExpression = new FuncExpression(func, Collections.singletonList(path));
          expressions.add(funcExpression);
          setSelectedFuncsAndExpression(func, funcExpression);
        });

    this.hasFunc = true;

    this.precision = precision;
    this.slideDistance = slideDistance;
    this.startKey = startKey;
    this.endKey = endKey;
    this.setHasDownsample(true);

    this.setFromSession(startKey, endKey);
  }

  private void setFromSession(long startKey, long endKey) {
    this.statementType = StatementType.SELECT;
    this.selectStatementType = SelectStatementType.UNARY;

    this.setAscending(true);
    this.setLimit(Integer.MAX_VALUE);
    this.setOffset(0);

    setFilter(
        new AndFilter(
            new ArrayList<>(
                Arrays.asList(new KeyFilter(Op.GE, startKey), new KeyFilter(Op.L, endKey)))));
    setHasValueFilter(true);
  }

  public static FuncType str2FuncType(String identifier) {
    switch (identifier.toLowerCase()) {
      case "first_value":
        return FuncType.FirstValue;
      case "last_value":
        return FuncType.LastValue;
      case "first":
        return FuncType.First;
      case "last":
        return FuncType.Last;
      case "min":
        return FuncType.Min;
      case "max":
        return FuncType.Max;
      case "avg":
        return FuncType.Avg;
      case "count":
        return FuncType.Count;
      case "sum":
        return FuncType.Sum;
      case "ratio":
        return FuncType.Ratio;
      case "": // no func
        return null;
      default:
        if (FunctionUtils.isRowToRowFunction(identifier)) {
          return FuncType.Udtf;
        } else if (FunctionUtils.isSetToRowFunction(identifier)) {
          return FuncType.Udaf;
        } else if (FunctionUtils.isSetToSetFunction(identifier)) {
          return FuncType.Udsf;
        }
        throw new SQLParserException(String.format("Unregister UDF function: %s.", identifier));
    }
  }

  public boolean hasFunc() {
    return hasFunc;
  }

  public void setHasFunc(boolean hasFunc) {
    this.hasFunc = hasFunc;
  }

  public boolean isDistinct() {
    return selectClause.isDistinct();
  }

  public void setDistinct(boolean distinct) {
    selectClause.setDistinct(distinct);
  }

  public boolean hasValueFilter() {
    return whereClause.hasValueFilter();
  }

  public void setHasValueFilter(boolean hasValueFilter) {
    setHasValueFilter(hasValueFilter);
  }

  public boolean hasDownsample() {
    return groupByClause.hasDownsample();
  }

  public void setHasDownsample(boolean hasDownsample) {
    this.groupByClause.setHasDownsample(hasDownsample);
  }

  public boolean hasGroupBy() {
    return groupByClause.hasGroupBy();
  }

  public void setHasGroupBy(boolean hasGroupBy) {
    this.groupByClause.setHasGroupBy(hasGroupBy);
  }

  public boolean hasJoinParts() {
    return fromClause.hasJoinParts();
  }

  public void setHasJoinParts(boolean hasJoinParts) {
    fromClause.setHasJoinParts(hasJoinParts);
  }

  public boolean hasValueToSelectedPath() {
    return selectClause.hasValueToSelectedPath();
  }

  public void setHasValueToSelectedPath(boolean hasValueToSelectedPath) {
    selectClause.setHasValueToSelectedPath(hasValueToSelectedPath);
  }

  public Map<String, List<FuncExpression>> getFuncExpressionMap() {
    return funcExpressionMap;
  }

  public List<BaseExpression> getBaseExpressionList() {
    return baseExpressionList;
  }

  public void addBaseExpression(BaseExpression baseExpression) {
    this.baseExpressionList.add(baseExpression);
  }

  public void setSelectedPaths(BaseExpression baseExpression) {
    this.setSelectedPaths(baseExpression, true);
  }

  public void setSelectedPaths(BaseExpression baseExpression, boolean addToPathSet) {
    this.baseExpressionList.add(baseExpression);

    if (addToPathSet) {
      this.pathSet.add(baseExpression.getPathName());
    }
  }

  public void setSelectedFuncsAndExpression(String func, FuncExpression expression) {
    setSelectedFuncsAndExpression(func, expression, true);
  }

  public void setSelectedFuncsAndExpression(
      String func, FuncExpression expression, boolean addToPathSet) {
    func = func.trim();

    List<FuncExpression> expressions = getFuncExpressionMap().get(func);
    if (expressions == null) {
      expressions = new ArrayList<>();
      expressions.add(expression);
      getFuncExpressionMap().put(func, expressions);
    } else {
      expressions.add(expression);
    }

    if (addToPathSet) {
      this.pathSet.addAll(expression.getColumns());
    }

    FuncType type = str2FuncType(func);
    if (type != null) {
      this.funcTypeSet.add(type);
    }
  }

  public Set<FuncType> getFuncTypeSet() {
    return funcTypeSet;
  }

  @Override
  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPathSet(String path) {
    this.pathSet.add(path);
  }

  public List<SubQueryFromPart> getSelectSubQueryParts() {
    return selectClause.getSelectSubQueryParts();
  }

  public void addSelectSubQueryPart(SubQueryFromPart selectSubQueryPart) {
    selectClause.addSelectSubQueryPart(selectSubQueryPart);
  }

  public List<FromPart> getFromParts() {
    return fromClause.getFromParts();
  }

  public FromPart getFromPart(int index) {
    return getFromParts().get(index);
  }

  public void setFromParts(List<FromPart> fromParts) {
    fromClause.setFromParts(fromParts);
  }

  public void addFromPart(FromPart fromPart) {
    getFromParts().add(fromPart);
  }

  public List<SubQueryFromPart> getWhereSubQueryParts() {
    return whereClause.getWhereSubQueryParts();
  }

  public void addWhereSubQueryPart(SubQueryFromPart whereSubQueryPart) {
    whereClause.addWhereSubQueryPart(whereSubQueryPart);
  }

  public void setGroupByPath(String path) {
    this.groupByClause.addGroupByPath(path);
  }

  public List<SubQueryFromPart> getHavingSubQueryParts() {
    return havingClause.getHavingSubQueryParts();
  }

  public void addHavingSubQueryPart(SubQueryFromPart havingSubQueryPart) {
    this.havingClause.addHavingSubQueryPart(havingSubQueryPart);
  }

  public List<String> getGroupByPaths() {
    return groupByClause.getGroupByPaths();
  }

  public void setOrderByPath(String orderByPath) {
    this.orderByClause.setOrderByPaths(orderByPath);
  }

  public Filter getFilter() {
    return whereClause.getFilter();
  }

  public void setFilter(Filter filter) {
    this.whereClause.setFilter(filter);
  }

  public TagFilter getTagFilter() {
    return whereClause.getTagFilter();
  }

  public void setTagFilter(TagFilter tagFilter) {
    whereClause.setTagFilter(tagFilter);
  }

  public Filter getHavingFilter() {
    return havingClause.getHavingFilter();
  }

  public void setHavingFilter(Filter havingFilter) {
    this.havingClause.setHavingFilter(havingFilter);
  }

  public long getStartKey() {
    return startKey;
  }

  public void setStartKey(long startKey) {
    this.startKey = startKey;
  }

  public long getEndKey() {
    return endKey;
  }

  public void setEndKey(long endKey) {
    this.endKey = endKey;
  }

  public long getPrecision() {
    return precision;
  }

  public void setPrecision(long precision) {
    this.precision = precision;
  }

  public long getSlideDistance() {
    return slideDistance;
  }

  public void setSlideDistance(long slideDistance) {
    this.slideDistance = slideDistance;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public void checkQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  @Override
  public List<Expression> getExpressions() {
    return expressions;
  }

  public void setExpression(Expression expression) {
    expressions.add(expression);
  }

  public Map<String, String> getSelectAliasMap() {
    Map<String, String> aliasMap = new HashMap<>();
    this.expressions.forEach(
        expression -> {
          if (expression.hasAlias()) {
            aliasMap.put(expression.getColumnName(), expression.getAlias());
          }
        });
    return aliasMap;
  }

  @Override
  public Map<String, String> getSubQueryAliasMap(String alias) {
    Map<String, String> aliasMap = new HashMap<>();
    expressions.forEach(
        expression -> {
          if (expression.hasAlias()) {
            aliasMap.put(expression.getAlias(), alias + DOT + expression.getAlias());
          } else {
            if (expression.getType().equals(Expression.ExpressionType.Binary)
                || expression.getType().equals(Expression.ExpressionType.Unary)) {
              aliasMap.put(
                  expression.getColumnName(),
                  alias + DOT + L_PARENTHESES + expression.getColumnName() + R_PARENTHESES);
            } else {
              aliasMap.put(expression.getColumnName(), alias + DOT + expression.getColumnName());
            }
          }
        });
    return aliasMap;
  }

  public boolean needRowTransform() {
    for (Expression expression : expressions) {
      if (!expression.getType().equals(Expression.ExpressionType.Base)
          && !expression.getType().equals(Expression.ExpressionType.Function)
          && !expression.getType().equals(Expression.ExpressionType.FromValue)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<String> calculatePrefixSet() {
    Set<String> prefixSet = new HashSet<>();
    getFromParts()
        .forEach(
            fromPart -> {
              prefixSet.addAll(fromPart.getPatterns());
            });
    expressions.forEach(
        expression -> {
          if (expression.hasAlias()) {
            prefixSet.add(expression.getAlias());
          } else if (!expression.getType().equals(Expression.ExpressionType.Base)) {
            prefixSet.add(expression.getColumnName());
          }
        });
    return new ArrayList<>(prefixSet);
  }

  @Override
  public void initFreeVariables() {
    if (freeVariables != null) {
      return;
    }
    Set<String> set = new HashSet<>();
    for (FromPart fromPart : getFromParts()) {
      set.addAll(fromPart.getFreeVariables());
    }
    for (SubQueryFromPart whereSubQueryPart : getWhereSubQueryParts()) {
      set.addAll(whereSubQueryPart.getSubQuery().getFreeVariables());
    }
    for (SubQueryFromPart havingSubQueryPart : getHavingSubQueryParts()) {
      set.addAll(havingSubQueryPart.getSubQuery().getFreeVariables());
    }
    for (SubQueryFromPart selectSubQueryPart : getSelectSubQueryParts()) {
      set.addAll(selectSubQueryPart.getSubQuery().getFreeVariables());
    }
    List<String> toBeRemove = new ArrayList<>();
    set.forEach(
        variable -> {
          if (!isFreeVariable(variable)) {
            toBeRemove.add(variable);
          }
        });
    toBeRemove.forEach(set::remove);

    toBeRemove.clear();
    set.addAll(FilterUtils.getAllPathsFromFilter(getFilter()));
    set.forEach(
        variable -> {
          if (hasAttributeInWhereSubQuery(variable)) {
            toBeRemove.add(variable);
          } else if (!isFreeVariable(variable)) {
            toBeRemove.add(variable);
          }
        });
    toBeRemove.forEach(set::remove);

    toBeRemove.clear();
    set.addAll(FilterUtils.getAllPathsFromFilter(getHavingFilter()));
    set.forEach(
        variable -> {
          if (hasAttributeInHavingSubQuery(variable)) {
            toBeRemove.add(variable);
          } else if (!isFreeVariable(variable)) {
            toBeRemove.add(variable);
          }
        });
    toBeRemove.forEach(set::remove);

    toBeRemove.clear();
    set.forEach(
        variable -> {
          for (Expression expression : expressions) {
            if (!expression.getType().equals(Expression.ExpressionType.Base)
                && variable.equals(expression.getColumnName())) {
              toBeRemove.add(variable);
            }
          }
        });
    toBeRemove.forEach(set::remove);

    freeVariables = new ArrayList<>(set);
  }

  public boolean isFreeVariable(String path) {
    return !hasAttribute(path, getFromParts().size());
  }

  public boolean hasAttribute(String path, int endIndexOfFromPart) {
    if (endIndexOfFromPart > getFromParts().size()) {
      throw new RuntimeException("index: " + endIndexOfFromPart + " overflow");
    }
    if (path.startsWith(MarkJoin.MARK_PREFIX)) {
      return true;
    }
    for (int i = 0; i < endIndexOfFromPart; i++) {
      for (String pattern : getFromParts().get(i).getPatterns()) {
        if (StringUtils.match(path, pattern)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean hasAttributeInWhereSubQuery(String path) {
    if (path.startsWith(MarkJoin.MARK_PREFIX)) {
      return true;
    }
    for (SubQueryFromPart whereSubQueryPart : getWhereSubQueryParts()) {
      for (String pattern : whereSubQueryPart.getPatterns()) {
        if (StringUtils.match(path, pattern)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean hasAttributeInHavingSubQuery(String path) {
    if (path.startsWith(MarkJoin.MARK_PREFIX)) {
      return true;
    }
    for (SubQueryFromPart havingSubQueryPart : getHavingSubQueryParts()) {
      for (String pattern : havingSubQueryPart.getPatterns()) {
        if (StringUtils.match(path, pattern)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isFromSinglePath() {
    return !hasJoinParts()
        && !getFromParts().isEmpty()
        && (getFromParts().get(0).getType().equals(FromPartType.Path)
            || getFromParts().get(0).getType().equals(FromPartType.Cte));
  }

  public void checkQueryType() {
    if (hasGroupBy()) {
      this.queryType = QueryType.GroupByQuery;
    } else if (hasFunc) {
      if (hasDownsample()) {
        this.queryType = QueryType.DownSampleQuery;
      } else {
        this.queryType = QueryType.AggregateQuery;
      }
    } else {
      if (hasDownsample()) {
        throw new SQLParserException(
            "Downsample clause cannot be used without aggregate function.");
      } else {
        this.queryType = QueryType.SimpleQuery;
      }
    }
    if (queryType == QueryType.AggregateQuery) {
      if (funcTypeSet.contains(FuncType.First) || funcTypeSet.contains(FuncType.Last)) {
        this.queryType = QueryType.LastFirstQuery;
      }
    }

    // calculate func type count
    int[] cntArr = new int[3];
    for (FuncType type : funcTypeSet) {
      if (FuncType.isRow2RowFunc(type)) {
        cntArr[0]++;
      } else if (FuncType.isSet2SetFunc(type)) {
        cntArr[1]++;
      } else if (FuncType.isSet2RowFunc(type)) {
        cntArr[2]++;
      }
    }
    int typeCnt = 0;
    for (int cnt : cntArr) {
      typeCnt += Math.min(1, cnt);
    }

    // SetToSet SetToRow RowToRow functions can not be mixed.
    if (typeCnt > 1) {
      throw new SQLParserException(
          "SetToSet/SetToRow/RowToRow functions can not be mixed in aggregate query.");
    }
    // SetToSet SetToRow functions and non-function modified path can not be mixed.
    if (typeCnt == 1 && !hasGroupBy() && cntArr[0] == 0 && !baseExpressionList.isEmpty()) {
      throw new SQLParserException(
          "SetToSet/SetToRow functions and non-function modified path can not be mixed.");
    }
    if (hasGroupBy() && (cntArr[0] > 0 || cntArr[1] > 0)) {
      throw new SQLParserException("Group by can not use SetToSet and RowToRow functions.");
    }
  }

  public enum QueryType {
    Unknown,
    SimpleQuery,
    AggregateQuery,
    LastFirstQuery,
    DownSampleQuery,
    GroupByQuery
  }
}
