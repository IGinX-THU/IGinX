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
package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OrderByClause {
  private final List<Expression> orderByExpressions;
  private final List<Boolean> ascendingList;
  private final Set<String> pathSet;

  public OrderByClause(List<Expression> orderByExpressions, List<Boolean> ascendingList) {
    this.orderByExpressions = orderByExpressions;
    this.ascendingList = ascendingList;
    this.pathSet = new HashSet<>();
  }

  public OrderByClause() {
    this.orderByExpressions = new ArrayList<>();
    this.ascendingList = new ArrayList<>();
    this.pathSet = new HashSet<>();
  }

  public List<Expression> getOrderByExpressions() {
    return orderByExpressions;
  }

  public List<Boolean> getAscendingList() {
    return ascendingList;
  }

  public void setAscendingList(boolean ascending) {
    this.ascendingList.add(ascending);
  }

  public void setOrderByExpr(Expression orderByExpr) {
    this.orderByExpressions.add(orderByExpr);
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }
}
