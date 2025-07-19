/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.sql.statement.select;

import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.L_PARENTHESES;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.R_PARENTHESES;

import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.*;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.CteFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.PathFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.select.subclause.*;
import cn.edu.tsinghua.iginx.sql.utils.ExpressionUtils;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
  public UnarySelectStatement(List<String> paths, AggregateType aggregateType) {
    this(false);

    if (aggregateType == AggregateType.LAST || aggregateType == AggregateType.FIRST) {
      setQueryType(QueryType.MappingQuery);
    } else {
      setQueryType(QueryType.AggregateQuery);
    }

    String func = aggregateType.toString().toLowerCase();
    paths.forEach(
        path -> {
          BaseExpression column = new BaseExpression(path);
          FuncExpression funcExpression =
              new FuncExpression(func, Collections.singletonList(column));
          addSelectClauseExpression(funcExpression);
          this.selectClause.addPath(path);
        });
  }

  public UnarySelectStatement(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType) {
    this(paths, aggregateType);
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
          BaseExpression column = new BaseExpression(path);
          FuncExpression funcExpression =
              new FuncExpression(func, Collections.singletonList(column));
          addSelectClauseExpression(funcExpression);
          this.selectClause.addPath(path);
        });

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

  public boolean isAllConstArith() {
    return selectClause.isAllConstArith();
  }

  public boolean isLastFirst() {
    return getQueryType().equals(QueryType.MappingQuery) && selectClause.isLastFirst();
  }

  public List<FuncExpression> getTargetTypeFuncExprList(MappingType mappingType) {
    return selectClause.getTargetTypeFuncExprList(mappingType);
  }

  public List<BaseExpression> getBaseExpressionList() {
    return getBaseExpressionList(false);
  }

  /**
   * 获取ExpressionList中获取所有的BaseExpression
   *
   * @param exceptFunc 是否不包括FuncExpression参数中的BaseExpression
   * @return BaseExpression列表
   */
  public List<BaseExpression> getBaseExpressionList(boolean exceptFunc) {
    return ExpressionUtils.getBaseExpressionList(getExpressions(), exceptFunc);
  }

  public List<SequenceExpression> getSequenceExpressionList() {
    return selectClause.getSequenceExpressionList();
  }

  @Override
  public Set<String> getPathSet() {
    Set<String> pathSet = new HashSet<>();
    pathSet.addAll(selectClause.getPathSet());
    pathSet.addAll(fromClause.getPathSet());
    pathSet.addAll(whereClause.getPathSet());
    pathSet.addAll(groupByClause.getPathSet());
    pathSet.addAll(havingClause.getPathSet());
    pathSet.addAll(orderByClause.getPathSet());
    return pathSet;
  }

  public void addSelectPath(String path) {
    selectClause.addPath(path);
  }

  public void addFromPath(String path) {
    fromClause.addPath(path);
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

  public void addOrderByPath(String path) {
    orderByClause.addPath(path);
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

  public List<SubQueryFromPart> getWhereSubQueryParts() {
    return whereClause.getWhereSubQueryParts();
  }

  public void addWhereSubQueryPart(SubQueryFromPart whereSubQueryPart) {
    whereClause.addWhereSubQueryPart(whereSubQueryPart);
  }

  public void setGroupByExpr(Expression expression) {
    this.groupByClause.addGroupByExpression(expression);
  }

  public List<SubQueryFromPart> getHavingSubQueryParts() {
    return havingClause.getHavingSubQueryParts();
  }

  public void addHavingSubQueryPart(SubQueryFromPart havingSubQueryPart) {
    this.havingClause.addHavingSubQueryPart(havingSubQueryPart);
  }

  public List<Expression> getGroupByExpressions() {
    return groupByClause.getGroupByExpressions();
  }

  public void setOrderByExpr(Expression orderByExpr) {
    super.setOrderByExpr(orderByExpr);
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

  @Override
  public boolean isSimpleQuery() {
    return groupByClause.getQueryType() == QueryType.SimpleQuery;
  }

  public QueryType getQueryType() {
    return groupByClause.getQueryType();
  }

  public void setQueryType(QueryType queryType) {
    groupByClause.setQueryType(queryType);
  }

  @Override
  public List<Expression> getExpressions() {
    return new ArrayList<>(selectClause.getExpressions());
  }

  @Override
  public UnarySelectStatement getFirstUnarySelectStatement() {
    return this;
  }

  public void addSelectClauseExpression(Expression expression) {
    selectClause.addExpression(expression);
  }

  public List<Pair<String, String>> getSelectAliasList() {
    List<Pair<String, String>> aliasList = new ArrayList<>();
    AtomicBoolean hasAlias = new AtomicBoolean(false);
    getExpressions()
        .forEach(
            expression -> {
              if (expression.hasAlias()) {
                aliasList.add(new Pair<>(expression.getColumnName(), expression.getAlias()));
                hasAlias.set(true);
              } else {
                aliasList.add(new Pair<>(expression.getColumnName(), expression.getColumnName()));
              }
            });
    return hasAlias.get() ? aliasList : Collections.emptyList();
  }

  @Override
  public List<Pair<String, String>> getSubQueryAliasList(String alias) {
    List<Pair<String, String>> aliasList = new ArrayList<>();
    getExpressions()
        .forEach(
            expression -> {
              if (expression.hasAlias()) {
                aliasList.add(
                    new Pair<>(expression.getAlias(), alias + DOT + expression.getAlias()));
              } else {
                if (expression.getType().equals(Expression.ExpressionType.Binary)
                    || expression.getType().equals(Expression.ExpressionType.Unary)) {
                  aliasList.add(
                      new Pair<>(
                          expression.getColumnName(),
                          alias
                              + DOT
                              + L_PARENTHESES
                              + expression.getColumnName()
                              + R_PARENTHESES));
                } else {
                  aliasList.add(
                      new Pair<>(
                          expression.getColumnName(), alias + DOT + expression.getColumnName()));
                }
              }
            });
    return aliasList;
  }

  public boolean needRowTransform() {
    for (Expression expression : getExpressions()) {
      if (getQueryType() == QueryType.GroupByQuery
          && groupByClause.getGroupByExpressions().stream()
              .anyMatch(e -> e.equalExceptAlias(expression))) {
        continue;
      }
      if (expression.getType().equals(Expression.ExpressionType.Function)) {
        FuncExpression funcExpression = (FuncExpression) expression;
        if (FunctionUtils.isRowToRowFunction(funcExpression.getFuncName())) {
          return true;
        }
      } else if (!expression.getType().equals(Expression.ExpressionType.Base)
          && !expression.getType().equals(Expression.ExpressionType.FromValue)
          && !expression.getType().equals(Expression.ExpressionType.Sequence)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<String> calculatePrefixSet() {
    Set<String> prefixSet = new HashSet<>();
    getFromParts().forEach(fromPart -> prefixSet.addAll(fromPart.getPatterns()));
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

  public String getOriginPath(String path) {
    if (path.equals("*") || path.startsWith(MarkJoin.MARK_PREFIX)) {
      return path;
    }
    for (FromPart fromPart : getFromParts()) {
      for (String pattern : fromPart.getPatterns()) {
        if (StringUtils.match(path, pattern)) {
          if (fromPart.hasAlias()) {
            if (!path.startsWith(fromPart.getPrefix())) {
              throw new RuntimeException(
                  String.format(
                      "prefix: %s not in path: %s, please check", fromPart.getPrefix(), path));
            }
            return path.replaceFirst(fromPart.getPrefix(), fromPart.getOriginPrefix());
          } else {
            return path;
          }
        } else if (OperatorUtils.covers(path, pattern)) {
          return PathUtils.recoverRenamedPattern(fromPart.getAliasList(), path).get(0);
        }
      }
    }
    return null;
  }

  public boolean isFromSinglePath() {
    return !hasJoinParts()
        && !getFromParts().isEmpty()
        && (getFromParts().get(0).getType().equals(FromPartType.Path)
            || getFromParts().get(0).getType().equals(FromPartType.Cte));
  }

  public void checkQueryType() {
    boolean isAllConstArith =
        getExpressions().stream().allMatch(ExpressionUtils::isConstantArithmeticExpr);
    selectClause.setAllConstArith(isAllConstArith);
    if (getFromParts().isEmpty() && !isAllConstArith) {
      throw new SQLParserException(
          "Statement without FROM clause should only contain constant arithmetic expressions.");
    }

    boolean isAllExprNeedNoPath =
        getExpressions().stream().allMatch(UnarySelectStatement::isNeedNoPath);
    if (isAllExprNeedNoPath) {
      fromClause
          .getFromParts()
          .forEach(
              fromPart -> {
                if (fromPart instanceof PathFromPart || fromPart instanceof CteFromPart) {
                  selectClause.addPath(fromPart.getOriginPrefix() + ".*");
                }
              });
    }

    Set<MappingType> typeList = new HashSet<>();
    for (Expression expression : getExpressions()) {
      typeList.add(ExpressionUtils.getExprMappingType(expression));
    }
    typeList.remove(null);

    if (hasGroupBy()) {
      if (typeList.contains(MappingType.Mapping)) {
        throw new SQLParserException("Group by can not use SetToSet functions.");
      }
      setQueryType(QueryType.GroupByQuery);
      return;
    }

    if (typeList.size() > 1) {
      throw new SQLParserException(
          "SetToSet/SetToRow/RowToRow functions can not be mixed in selected expressions.");
    }
    if (typeList.isEmpty()) {
      if (hasDownsample()) {
        throw new SQLParserException(
            "Downsample clause cannot be used without aggregate function.");
      }
      setQueryType(QueryType.SimpleQuery);
      return;
    }

    MappingType type = typeList.iterator().next();
    if (hasDownsample()) {
      if (type == MappingType.Mapping) {
        throw new SQLParserException("Downsample clause can not use SetToSet functions.");
      } else if (type == MappingType.RowMapping) {
        if (getTargetTypeFuncExprList(MappingType.RowMapping).isEmpty()) {
          throw new SQLParserException(
              "Downsample clause cannot be used without aggregate function.");
        } else {
          throw new SQLParserException("Downsample clause can not use RowToRow functions.");
        }
      }
      setQueryType(QueryType.DownSampleQuery);
    } else if (type == MappingType.SetMapping) {
      setQueryType(QueryType.AggregateQuery);
    } else if (type == MappingType.Mapping) {
      setQueryType(QueryType.MappingQuery);
    } else {
      setQueryType(QueryType.SimpleQuery);
    }
  }

  private static boolean isNeedNoPath(Expression expression) {
    if (expression instanceof KeyExpression || expression instanceof SequenceExpression) {
      return true;
    }
    return ExpressionUtils.isConstantArithmeticExpr(expression);
  }

  public enum QueryType {
    Unknown,
    SimpleQuery,
    AggregateQuery,
    MappingQuery,
    DownSampleQuery,
    GroupByQuery
  }
}
