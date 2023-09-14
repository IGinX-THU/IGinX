package cn.edu.tsinghua.iginx.sql.statement.selectstatement;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.L_PARENTHESES;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.R_PARENTHESES;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.FuncExpression;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
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
  private boolean isDistinct = false;
  private boolean hasJoinParts = false;
  private boolean hasValueFilter;
  private boolean hasDownsample;
  private boolean hasGroupBy;
  private boolean hasValueToSelectedPath = false;

  private final List<Expression> expressions;
  private final Map<String, List<FuncExpression>> funcExpressionMap;
  private final List<BaseExpression> baseExpressionList;
  private final Set<FuncType> funcTypeSet;
  private final Set<String> pathSet;
  private final List<SubQueryFromPart> selectSubQueryParts;
  private List<FromPart> fromParts;
  private final List<SubQueryFromPart> whereSubQueryParts;
  private final List<String> groupByPaths;
  private final List<SubQueryFromPart> havingSubQueryParts;
  private Filter filter;
  private Filter havingFilter;
  private TagFilter tagFilter;
  private long precision;
  private long startKey;
  private long endKey;
  private long slideDistance;
  private List<Integer> layers;

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
    this.selectSubQueryParts = new ArrayList<>();
    this.fromParts = new ArrayList<>();
    this.whereSubQueryParts = new ArrayList<>();
    this.groupByPaths = new ArrayList<>();
    this.havingSubQueryParts = new ArrayList<>();
    this.layers = new ArrayList<>();
  }

  // simple query
  public UnarySelectStatement(List<String> paths, long startKey, long endKey) {
    super(false);
    this.selectStatementType = SelectStatementType.UNARY;
    this.queryType = QueryType.SimpleQuery;

    this.pathSet = new HashSet<>();
    this.expressions = new ArrayList<>();
    this.funcExpressionMap = new HashMap<>();
    this.baseExpressionList = new ArrayList<>();
    this.selectSubQueryParts = new ArrayList<>();
    this.fromParts = new ArrayList<>();
    this.whereSubQueryParts = new ArrayList<>();
    this.groupByPaths = new ArrayList<>();
    this.havingSubQueryParts = new ArrayList<>();
    this.funcTypeSet = new HashSet<>();

    paths.forEach(
        path -> {
          BaseExpression baseExpression = new BaseExpression(path);
          expressions.add(baseExpression);
          setSelectedPaths(baseExpression);
        });
    this.hasFunc = false;
    this.hasGroupBy = false;

    this.setFromSession(startKey, endKey);
  }

  // aggregate query
  public UnarySelectStatement(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType) {
    super(false);
    this.selectStatementType = SelectStatementType.UNARY;
    if (aggregateType == AggregateType.LAST || aggregateType == AggregateType.FIRST) {
      this.queryType = QueryType.LastFirstQuery;
    } else {
      this.queryType = QueryType.AggregateQuery;
    }

    this.pathSet = new HashSet<>();
    this.expressions = new ArrayList<>();
    this.funcExpressionMap = new HashMap<>();
    this.baseExpressionList = new ArrayList<>();
    this.selectSubQueryParts = new ArrayList<>();
    this.fromParts = new ArrayList<>();
    this.whereSubQueryParts = new ArrayList<>();
    this.groupByPaths = new ArrayList<>();
    this.havingSubQueryParts = new ArrayList<>();
    this.funcTypeSet = new HashSet<>();

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          FuncExpression funcExpression = new FuncExpression(func, Collections.singletonList(path));
          expressions.add(funcExpression);
          setSelectedFuncsAndPaths(func, funcExpression);
        });

    this.hasFunc = true;
    this.hasGroupBy = false;

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
    super(false);
    this.selectStatementType = SelectStatementType.UNARY;
    this.queryType = QueryType.DownSampleQuery;

    this.pathSet = new HashSet<>();
    this.expressions = new ArrayList<>();
    this.funcExpressionMap = new HashMap<>();
    this.baseExpressionList = new ArrayList<>();
    this.selectSubQueryParts = new ArrayList<>();
    this.fromParts = new ArrayList<>();
    this.whereSubQueryParts = new ArrayList<>();
    this.groupByPaths = new ArrayList<>();
    this.havingSubQueryParts = new ArrayList<>();
    this.funcTypeSet = new HashSet<>();

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          FuncExpression funcExpression = new FuncExpression(func, Collections.singletonList(path));
          expressions.add(funcExpression);
          setSelectedFuncsAndPaths(func, funcExpression);
        });

    this.hasFunc = true;
    this.hasGroupBy = false;

    this.precision = precision;
    this.slideDistance = slideDistance;
    this.startKey = startKey;
    this.endKey = endKey;
    this.hasDownsample = true;

    this.setFromSession(startKey, endKey);
  }

  private void setFromSession(long startKey, long endKey) {
    this.statementType = StatementType.SELECT;
    this.selectStatementType = SelectStatementType.UNARY;

    this.ascending = true;
    this.limit = Integer.MAX_VALUE;
    this.offset = 0;

    this.filter =
        new AndFilter(
            new ArrayList<>(
                Arrays.asList(new KeyFilter(Op.GE, startKey), new KeyFilter(Op.L, endKey))));
    this.hasValueFilter = true;
    this.layers = new ArrayList<>();
  }

  public static FuncType str2FuncType(String str) {
    String identifier = str.toLowerCase();
    switch (identifier) {
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
    return isDistinct;
  }

  public void setDistinct(boolean distinct) {
    isDistinct = distinct;
  }

  public boolean hasValueFilter() {
    return hasValueFilter;
  }

  public void setHasValueFilter(boolean hasValueFilter) {
    this.hasValueFilter = hasValueFilter;
  }

  public boolean hasDownsample() {
    return hasDownsample;
  }

  public void setHasDownsample(boolean hasDownsample) {
    this.hasDownsample = hasDownsample;
  }

  public boolean hasGroupBy() {
    return hasGroupBy;
  }

  public void setHasGroupBy(boolean hasGroupBy) {
    this.hasGroupBy = hasGroupBy;
  }

  public boolean hasJoinParts() {
    return hasJoinParts;
  }

  public void setHasJoinParts(boolean hasJoinParts) {
    this.hasJoinParts = hasJoinParts;
  }

  public boolean hasValueToSelectedPath() {
    return hasValueToSelectedPath;
  }

  public void setHasValueToSelectedPath(boolean hasValueToSelectedPath) {
    this.hasValueToSelectedPath = hasValueToSelectedPath;
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

  public void setSelectedFuncsAndPaths(String func, FuncExpression expression) {
    setSelectedFuncsAndPaths(func, expression, true);
  }

  public void setSelectedFuncsAndPaths(
      String func, FuncExpression expression, boolean addToPathSet) {
    func = func.trim().toLowerCase();

    List<FuncExpression> expressions = this.funcExpressionMap.get(func);
    if (expressions == null) {
      expressions = new ArrayList<>();
      expressions.add(expression);
      this.funcExpressionMap.put(func, expressions);
    } else {
      expressions.add(expression);
    }

    if (addToPathSet) {
      this.pathSet.addAll(expression.getParams());
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

  public void setPathSet(String path) {
    this.pathSet.add(path);
  }

  public List<SubQueryFromPart> getSelectSubQueryParts() {
    return selectSubQueryParts;
  }

  public void addSelectSubQueryPart(SubQueryFromPart selectSubQueryPart) {
    this.selectSubQueryParts.add(selectSubQueryPart);
  }

  public List<FromPart> getFromParts() {
    return fromParts;
  }

  public void setFromParts(List<FromPart> fromParts) {
    this.fromParts = fromParts;
  }

  public void addFromPart(FromPart fromPart) {
    this.fromParts.add(fromPart);
  }

  public List<SubQueryFromPart> getWhereSubQueryParts() {
    return whereSubQueryParts;
  }

  public void addWhereSubQueryPart(SubQueryFromPart whereSubQueryPart) {
    this.whereSubQueryParts.add(whereSubQueryPart);
  }

  public void setGroupByPath(String path) {
    this.groupByPaths.add(path);
  }

  public List<SubQueryFromPart> getHavingSubQueryParts() {
    return havingSubQueryParts;
  }

  public void addHavingSubQueryPart(SubQueryFromPart havingSubQueryPart) {
    this.havingSubQueryParts.add(havingSubQueryPart);
  }

  public List<String> getGroupByPaths() {
    return groupByPaths;
  }

  public void setOrderByPath(String orderByPath) {
    this.orderByPaths.add(orderByPath);
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  public Filter getHavingFilter() {
    return havingFilter;
  }

  public void setHavingFilter(Filter havingFilter) {
    this.havingFilter = havingFilter;
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

  public List<Integer> getLayers() {
    return layers;
  }

  public void setLayer(Integer layer) {
    this.layers.add(layer);
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

  public Map<String, String> getFromPathAliasMap(String originPrefix, String alias) {
    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put(originPrefix + ALL_PATH_SUFFIX, alias + ALL_PATH_SUFFIX);
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
    fromParts.forEach(
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
    for (FromPart fromPart : fromParts) {
      set.addAll(fromPart.getFreeVariables());
    }
    for (SubQueryFromPart whereSubQueryPart : whereSubQueryParts) {
      set.addAll(whereSubQueryPart.getSubQuery().getFreeVariables());
    }
    for (SubQueryFromPart havingSubQueryPart : havingSubQueryParts) {
      set.addAll(havingSubQueryPart.getSubQuery().getFreeVariables());
    }
    for (SubQueryFromPart selectSubQueryPart : selectSubQueryParts) {
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
    set.addAll(FilterUtils.getAllPathsFromFilter(filter));
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
    set.addAll(FilterUtils.getAllPathsFromFilter(havingFilter));
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
    return !hasAttribute(path, fromParts.size());
  }

  public boolean hasAttribute(String path, int endIndexOfFromPart) {
    if (endIndexOfFromPart > fromParts.size()) {
      throw new RuntimeException("index: " + endIndexOfFromPart + " overflow");
    }
    if (path.startsWith(MarkJoin.MARK_PREFIX)) {
      return true;
    }
    for (int i = 0; i < endIndexOfFromPart; i++) {
      for (String pattern : fromParts.get(i).getPatterns()) {
        if (pattern.endsWith(Constants.ALL_PATH_SUFFIX)) {
          pattern = pattern.substring(0, pattern.length() - 1);
        }
        if (path.startsWith(pattern)) {
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
    for (SubQueryFromPart whereSubQueryPart : whereSubQueryParts) {
      for (String pattern : whereSubQueryPart.getPatterns()) {
        if (pattern.endsWith(Constants.ALL_PATH_SUFFIX)) {
          pattern = pattern.substring(0, pattern.length() - 1);
        }
        if (path.startsWith(pattern)) {
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
    for (SubQueryFromPart havingSubQueryPart : havingSubQueryParts) {
      for (String pattern : havingSubQueryPart.getPatterns()) {
        if (pattern.endsWith(Constants.ALL_PATH_SUFFIX)) {
          pattern = pattern.substring(0, pattern.length() - 1);
        }
        if (path.startsWith(pattern)) {
          return true;
        }
      }
    }
    return false;
  }

  public void checkQueryType() {
    if (hasGroupBy) {
      this.queryType = QueryType.GroupByQuery;
    } else if (hasFunc) {
      if (hasDownsample) {
        this.queryType = QueryType.DownSampleQuery;
      } else {
        this.queryType = QueryType.AggregateQuery;
      }
    } else {
      if (hasDownsample) {
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
    if (typeCnt == 1 && !hasGroupBy && cntArr[0] == 0 && !baseExpressionList.isEmpty()) {
      throw new SQLParserException(
          "SetToSet/SetToRow functions and non-function modified path can not be mixed.");
    }
    if (hasGroupBy && (cntArr[0] > 0 || cntArr[1] > 0)) {
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
