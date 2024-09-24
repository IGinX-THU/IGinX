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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import java.util.ArrayList;
import java.util.List;

public class OrderByClause {
  private final List<String> orderByPaths;
  private final List<Boolean> ascendingList;

  public OrderByClause(List<String> orderByPaths, List<Boolean> ascendingList) {
    this.orderByPaths = orderByPaths;
    this.ascendingList = ascendingList;
  }

  public OrderByClause() {
    this.orderByPaths = new ArrayList<>();
    this.ascendingList = new ArrayList<>();
  }

  public List<String> getOrderByPaths() {
    return orderByPaths;
  }

  public List<Boolean> getAscendingList() {
    return ascendingList;
  }

  public void setAscendingList(boolean ascending) {
    this.ascendingList.add(ascending);
  }

  public void setOrderByPaths(String orderByPath) {
    this.orderByPaths.add(orderByPath);
  }
}
