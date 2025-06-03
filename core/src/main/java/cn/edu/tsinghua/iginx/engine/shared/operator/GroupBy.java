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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GroupBy extends AbstractUnaryOperator {

  private final List<Expression> groupByExpressions;

  private final List<String> groupByCols;

  private final List<FunctionCall> functionCallList;

  public GroupBy(
      Source source, List<Expression> groupByExpressions, List<FunctionCall> functionCallList) {
    super(OperatorType.GroupBy, source);
    if (groupByExpressions == null || groupByExpressions.isEmpty()) {
      throw new IllegalArgumentException("groupByCols shouldn't be null");
    }
    this.groupByExpressions = groupByExpressions;
    this.groupByCols =
        groupByExpressions.stream().map(Expression::getColumnName).collect(Collectors.toList());
    this.functionCallList = functionCallList;
  }

  public List<Expression> getGroupByExpressions() {
    return groupByExpressions;
  }

  public List<String> getGroupByCols() {
    return groupByCols;
  }

  public void setGroupByExpression(int i, Expression expression) {
    groupByExpressions.set(i, expression);
    groupByCols.set(i, expression.getColumnName());
  }

  public void updateGroupByCols() {
    groupByCols.clear();
    groupByCols.addAll(
        groupByExpressions.stream().map(Expression::getColumnName).collect(Collectors.toList()));
  }

  public List<FunctionCall> getFunctionCallList() {
    return functionCallList;
  }

  @Override
  public Operator copy() {
    List<Expression> copyGroupByExpressions = new ArrayList<>(groupByExpressions.size());
    for (Expression expression : groupByExpressions) {
      copyGroupByExpressions.add(ExprUtils.copy(expression));
    }
    return new GroupBy(
        getSource().copy(),
        copyGroupByExpressions,
        functionCallList.stream().map(FunctionCall::copy).collect(Collectors.toList()));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    List<Expression> copyGroupByExpressions = new ArrayList<>(groupByExpressions.size());
    for (Expression expression : groupByExpressions) {
      copyGroupByExpressions.add(ExprUtils.copy(expression));
    }
    return new GroupBy(source, copyGroupByExpressions, new ArrayList<>(functionCallList));
  }

  public boolean isDistinct() {
    // 所有的functionCall的distinct属性都相同，所以只需要看第一个就可以了
    if (functionCallList.size() > 0) {
      return functionCallList.get(0).getParams().isDistinct();
    }
    return false;
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("GroupByCols: ").append(String.join(",", groupByCols));
    if (functionCallList != null && !functionCallList.isEmpty()) {
      builder.append(", FuncList(Name, FuncType): ");
      for (FunctionCall functionCall : functionCallList) {
        builder.append(functionCall.getNameAndFuncTypeStr()).append(",");
      }
      builder.append(" MappingType: ");
      builder.append(functionCallList.get(0).getFunction().getMappingType());
      if (isDistinct()) {
        builder.append(" isDistinct: true");
      } else {
        builder.append(" isDistinct: false");
      }
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    GroupBy that = (GroupBy) object;
    if (this.groupByExpressions.size() != that.groupByExpressions.size()) {
      return false;
    }
    for (int i = 0; i < this.groupByExpressions.size(); i++) {
      if (!this.groupByExpressions.get(i).equalExceptAlias(that.groupByExpressions.get(i))) {
        return false;
      }
    }
    return functionCallList.equals(that.functionCallList);
  }
}
