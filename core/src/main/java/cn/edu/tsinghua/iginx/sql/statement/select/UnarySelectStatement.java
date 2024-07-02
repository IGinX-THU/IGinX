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
package cn.edu.tsinghua.iginx.sql.statement.select;

import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.L_PARENTHESES;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.R_PARENTHESES;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.select.subclause.*;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;

public class UnarySelectStatement extends SelectStatement {

  private final SelectClause selectClause;
  private final FromClause fromClause;
  private final WhereClause whereClause;
  private final GroupByClause groupByClause;
  private final HavingClause havingClause;

  public UnarySelectStatement() {
    this(false);
  }

  public UnarySelectStatement(boolean isSubQuery) {
    super(isSubQuery);
    this.selectStatementType = SelectStatementType.UNARY;

    this.selectClause = new SelectClause();
    this.fromClause = new FromClause();
    this.whereClause = new WhereClause();
    this.groupByClause = new GroupByClause();
    this.havingClause = new HavingClause();
  }

  // simple query
  public UnarySelectStatement(List<String> paths, long startKey, long endKey) {
    this(false);

    setQueryType(QueryType.SimpleQuery);

    paths.forEach(
        path -> {
          BaseExpression baseExpression = new BaseExpression(path);
          addSelectClauseExpression(baseExpression);
          addSelectPath(path);
        });

    this.setFromSession(startKey, endKey);
  }

  // aggregate query
  public UnarySelectStatement(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType) {
    this(false);

    if (aggregateType == AggregateType.LAST || aggregateType == AggregateType.FIRST) {
      setQueryType(QueryType.LastFirstQuery);
    } else {
      setQueryType(QueryType.AggregateQuery);
    }

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          FuncExpression funcExpression = new FuncExpression(func, Collections.singletonList(path));
          addSelectClauseExpression(funcExpression);
          setSelectedFuncsAndExpression(func, funcExpression);
        });

    setHasFunc(true);

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
    setQueryType(QueryType.DownSampleQuery);

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          FuncExpression funcExpression = new FuncExpression(func, Collections.singletonList(path));
          addSelectClauseExpression(funcExpression);
          setSelectedFuncsAndExpression(func, funcExpression);
        });

    setHasFunc(true);

    setPrecision(precision);
    setSlideDistance(slideDistance);
    setStartKey(startKey);
    setEndKey(endKey);
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

  public boolean hasFunc() {
    return selectClause.hasFunc();
  }

  public void setHasFunc(boolean hasFunc) {
    selectClause.setHasFunc(hasFunc);
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
    whereClause.setHasValueFilter(hasValueFilter);
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
    return selectClause.getFuncExpressionMap();
  }

  public void addFuncExpressionMap(String func, List<FuncExpression> expressions) {
    selectClause.addFuncExpressionMap(func, expressions);
  }

  /**
   * 获取ExpressionList中获取所有的BaseExpression，包括嵌套的BaseExpression
   *
   * @return BaseExpression列表
   */
  public List<BaseExpression> getBaseExpressionList() {
    List<BaseExpression> baseExpressionList = new ArrayList<>();
    Queue<Expression> queue = new LinkedList<>(getExpressions());
    while (!queue.isEmpty()) {
      Expression expression = queue.poll();
      switch (expression.getType()) {
        case Base:
          baseExpressionList.add((BaseExpression) expression);
          break;
        case Unary:
          queue.add(((UnaryExpression) expression).getExpression());
          break;
        case Bracket:
          queue.add(((BracketExpression) expression).getExpression());
          break;
        case Binary:
          queue.add(((BinaryExpression) expression).getLeftExpression());
          queue.add(((BinaryExpression) expression).getRightExpression());
          break;
      }
    }
    return baseExpressionList;
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
      addFuncExpressionMap(func, expressions);
    } else {
      expressions.add(expression);
    }

    if (addToPathSet) {
      this.selectClause.addAllPath(expression.getColumns());
    }
  }

  public Set<FuncType> getFuncTypeSet() {
    return selectClause.getFuncTypeSet();
  }

  public boolean containsFuncType(FuncType funcType) {
    return selectClause.getFuncTypeSet().contains(funcType);
  }

  @Override
  public Set<String> getPathSet() {
    Set<String> pathSet = new HashSet<>();
    pathSet.addAll(selectClause.getPathSet());
    pathSet.addAll(whereClause.getPathSet());
    pathSet.addAll(groupByClause.getPathSet());
    pathSet.addAll(havingClause.getPathSet());
    return pathSet;
  }

  public void addSelectPath(String path) {
    selectClause.addPath(path);
  }

  public void addGroupByPath(String path) {
    groupByClause.addPath(path);
  }

  public void addWherePath(String path) {
    whereClause.addPath(path);
  }

  public void addHavingPath(String path) {
    havingClause.addPath(path);
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
    return fromClause.getFromParts().get(index);
  }

  public void setFromParts(List<FromPart> fromParts) {
    fromClause.setFromParts(fromParts);
  }

  public void addFromPart(FromPart fromPart) {
    fromClause.addFromPart(fromPart);
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
    return whereClause.getStartKey();
  }

  public void setStartKey(long startKey) {
    whereClause.setStartKey(startKey);
  }

  public long getEndKey() {
    return whereClause.getEndKey();
  }

  public void setEndKey(long endKey) {
    whereClause.setEndKey(endKey);
  }

  public long getPrecision() {
    return groupByClause.getPrecision();
  }

  public void setPrecision(long precision) {
    this.groupByClause.setPrecision(precision);
  }

  public long getSlideDistance() {
    return groupByClause.getSlideDistance();
  }

  public void setSlideDistance(long slideDistance) {
    this.groupByClause.setSlideDistance(slideDistance);
  }

  public QueryType getQueryType() {
    return groupByClause.getQueryType();
  }

  public void setQueryType(QueryType queryType) {
    groupByClause.setQueryType(queryType);
  }

  @Override
  public List<Expression> getExpressions() {
    List<Expression> expressions = new ArrayList<>();
    expressions.addAll(selectClause.getExpressions());
    return expressions;
  }

  public void addSelectClauseExpression(Expression expression) {
    selectClause.addExpression(expression);
  }

  public Map<String, String> getSelectAliasMap() {
    Map<String, String> aliasMap = new HashMap<>();
    getExpressions()
        .forEach(
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
    getExpressions()
        .forEach(
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
                  aliasMap.put(
                      expression.getColumnName(), alias + DOT + expression.getColumnName());
                }
              }
            });
    return aliasMap;
  }

  public boolean needRowTransform() {
    for (Expression expression : getExpressions()) {
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
    getExpressions()
        .forEach(
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
          for (Expression expression : getExpressions()) {
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
      setQueryType(QueryType.GroupByQuery);
    } else if (hasFunc()) {
      if (hasDownsample()) {
        setQueryType(QueryType.DownSampleQuery);
      } else {
        setQueryType(QueryType.AggregateQuery);
      }
    } else {
      if (hasDownsample()) {
        throw new SQLParserException(
            "Downsample clause cannot be used without aggregate function.");
      } else {
        setQueryType(QueryType.SimpleQuery);
      }
    }
    if (getQueryType() == QueryType.AggregateQuery) {
      if (containsFuncType(FuncType.First) || containsFuncType(FuncType.Last)) {
        setQueryType(QueryType.LastFirstQuery);
      }
    }

    // calculate func type count
    int[] cntArr = new int[3];
    for (FuncType type : getFuncTypeSet()) {
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
    if (typeCnt == 1 && !hasGroupBy() && cntArr[0] == 0 && !getBaseExpressionList().isEmpty()) {
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
