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
package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.*;

public class SelectClause {
  private final List<SubQueryFromPart> selectSubQueryParts;
  private boolean isDistinct = false;
  private boolean hasValueToSelectedPath = false;
  private final Map<String, List<FuncExpression>> funcExpressionMap;
  private boolean hasFunc = false;
  private final List<Expression> expressions;
  private final Set<String> pathSet;

  private static final Map<String, FuncType> str2TypeMap = new HashMap<>();

  static {
    str2TypeMap.put("first_value", FuncType.FirstValue);
    str2TypeMap.put("last_value", FuncType.LastValue);
    str2TypeMap.put("first", FuncType.First);
    str2TypeMap.put("last", FuncType.Last);
    str2TypeMap.put("min", FuncType.Min);
    str2TypeMap.put("max", FuncType.Max);
    str2TypeMap.put("avg", FuncType.Avg);
    str2TypeMap.put("count", FuncType.Count);
    str2TypeMap.put("sum", FuncType.Sum);
    str2TypeMap.put("ratio", FuncType.Ratio);
    str2TypeMap.put("", null);
  }

  public SelectClause() {
    this.selectSubQueryParts = new ArrayList<>();
    this.funcExpressionMap = new HashMap<>();
    this.expressions = new ArrayList<>();
    this.pathSet = new HashSet<>();
  }

  public void addSelectSubQueryPart(SubQueryFromPart selectSubQueryPart) {
    this.selectSubQueryParts.add(selectSubQueryPart);
  }

  public List<SubQueryFromPart> getSelectSubQueryParts() {
    return selectSubQueryParts;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean distinct) {
    isDistinct = distinct;
  }

  public boolean hasValueToSelectedPath() {
    return hasValueToSelectedPath;
  }

  public void setHasValueToSelectedPath(boolean hasValueToSelectedPath) {
    this.hasValueToSelectedPath = hasValueToSelectedPath;
  }

  public void addFuncExpressionMap(String func, List<FuncExpression> expressions) {
    funcExpressionMap.put(func, expressions);
  }

  public Map<String, List<FuncExpression>> getFuncExpressionMap() {
    return funcExpressionMap;
  }

  /**
   * 通过函数名，以及str2FuncType函数，获取函数类型的集合
   *
   * @return 函数类型的集合
   */
  public Set<FuncType> getFuncTypeSet() {
    Set<FuncType> funcTypeSet = new HashSet<>();
    for (Map.Entry<String, List<FuncExpression>> entry : funcExpressionMap.entrySet()) {
      String func = entry.getKey();
      funcTypeSet.add(str2FuncType(func));
    }
    return funcTypeSet;
  }

  public boolean containsFuncType(FuncType funcType) {
    for (Map.Entry<String, List<FuncExpression>> entry : funcExpressionMap.entrySet()) {
      String func = entry.getKey();
      if (str2FuncType(func) == funcType) {
        return true;
      }
    }
    return false;
  }

  public static FuncType str2FuncType(String identifier) {
    String lowerCaseIdentifier = identifier.toLowerCase();

    if (str2TypeMap.containsKey(lowerCaseIdentifier)) {
      return str2TypeMap.get(lowerCaseIdentifier);
    } else {
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

  public void addExpression(Expression expression) {
    expressions.add(expression);
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addAllPath(Collection<String> paths) {
    this.pathSet.addAll(paths);
  }

  public void addPath(String path) {
    pathSet.add(path);
  }
}
