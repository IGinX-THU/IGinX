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
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Sort extends AbstractUnaryOperator {

  private final List<Expression> sortByExpressions;

  private final List<String> sortByCols;

  private final List<SortType> sortTypes;

  public Sort(Source source, List<Expression> sortByExpressions, List<SortType> sortTypes) {
    super(OperatorType.Sort, source);
    if (sortByExpressions == null || sortByExpressions.isEmpty()) {
      throw new IllegalArgumentException("sortBy shouldn't be null");
    }
    if (sortTypes == null || sortTypes.isEmpty()) {
      throw new IllegalArgumentException("sortType shouldn't be null");
    }
    this.sortByExpressions = sortByExpressions;
    this.sortByCols =
        sortByExpressions.stream().map(Expression::getColumnName).collect(Collectors.toList());
    this.sortTypes = sortTypes;
  }

  public List<Expression> getSortByExpressions() {
    return sortByExpressions;
  }

  public List<String> getSortByCols() {
    return sortByCols;
  }

  public List<SortType> getSortTypes() {
    return sortTypes;
  }

  public List<Boolean> getAscendingList() {
    List<Boolean> ascendingList = new ArrayList<>(sortTypes.size());
    for (SortType sortType : sortTypes) {
      ascendingList.add(sortType == SortType.ASC);
    }
    return ascendingList;
  }

  @Override
  public Operator copy() {
    List<Expression> copySortByExpressions = new ArrayList<>(sortByExpressions.size());
    for (Expression expression : sortByExpressions) {
      copySortByExpressions.add(ExprUtils.copy(expression));
    }
    return new Sort(getSource().copy(), copySortByExpressions, new ArrayList<>(sortTypes));
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    List<Expression> copySortByExpressions = new ArrayList<>(sortByExpressions.size());
    for (Expression expression : sortByExpressions) {
      copySortByExpressions.add(ExprUtils.copy(expression));
    }
    return new Sort(source, copySortByExpressions, new ArrayList<>(sortTypes));
  }

  public enum SortType {
    ASC,
    DESC
  }

  @Override
  public String getInfo() {
    return "SortBy: "
        + String.join(",", sortByCols)
        + ", SortType: "
        + sortTypes.stream().map(String::valueOf).collect(Collectors.joining(","));
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Sort sort = (Sort) object;
    if (this.sortByExpressions.size() != sort.sortByExpressions.size()) {
      return false;
    }
    for (int i = 0; i < this.sortByExpressions.size(); i++) {
      if (!this.sortByExpressions.get(i).equalExceptAlias(sort.sortByExpressions.get(i))) {
        return false;
      }
    }
    return sortTypes.equals(sort.sortTypes);
  }
}
