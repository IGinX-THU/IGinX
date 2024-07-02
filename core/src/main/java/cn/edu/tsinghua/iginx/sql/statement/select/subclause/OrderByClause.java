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

import java.util.ArrayList;
import java.util.List;

public class OrderByClause {
  private final List<String> orderByPaths;
  private boolean ascending;

  public OrderByClause(List<String> orderByPaths, boolean ascending) {
    this.orderByPaths = orderByPaths;
    this.ascending = ascending;
  }

  public OrderByClause() {
    this.orderByPaths = new ArrayList<>();
    this.ascending = true;
  }

  public List<String> getOrderByPaths() {
    return orderByPaths;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public void setOrderByPaths(String orderByPath) {
    this.orderByPaths.add(orderByPath);
  }
}
