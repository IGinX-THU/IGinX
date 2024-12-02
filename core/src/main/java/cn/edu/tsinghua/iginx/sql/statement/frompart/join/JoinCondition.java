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
package cn.edu.tsinghua.iginx.sql.statement.frompart.join;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class JoinCondition {

  private final JoinType joinType;
  private final Filter filter;
  private final List<String> joinColumns;
  private final boolean isJoinByKey;
  private final String markColumn;
  private final boolean isAntiJoin;

  public JoinCondition() {
    this(JoinType.CrossJoin, null, Collections.emptyList(), false);
  }

  public JoinCondition(JoinType joinType, Filter filter) {
    this(joinType, filter, Collections.emptyList(), false);
  }

  public JoinCondition(JoinType joinType, Filter filter, List<String> joinColumns) {
    this(joinType, filter, joinColumns, false);
  }

  public JoinCondition(
      JoinType joinType, Filter filter, List<String> joinColumns, boolean isJoinByKey) {
    this.joinType = joinType;
    this.filter = filter;
    this.joinColumns = joinColumns;
    this.isJoinByKey = isJoinByKey;
    this.markColumn = null;
    this.isAntiJoin = false;
  }

  public JoinCondition(JoinType joinType, Filter filter, String markColumn, boolean isAntiJoin) {
    this.joinType = joinType;
    this.filter = filter;
    this.joinColumns = new ArrayList<>();
    this.isJoinByKey = false;
    this.markColumn = markColumn;
    this.isAntiJoin = isAntiJoin;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public Filter getFilter() {
    return filter;
  }

  public List<String> getJoinColumns() {
    return joinColumns;
  }

  public boolean isJoinByKey() {
    return isJoinByKey;
  }

  public String getMarkColumn() {
    return markColumn;
  }

  public boolean isAntiJoin() {
    return isAntiJoin;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinCondition joinCondition = (JoinCondition) o;
    return joinType == joinCondition.joinType
        && Objects.equals(filter, joinCondition.filter)
        && Objects.equals(joinColumns, joinCondition.joinColumns)
        && isJoinByKey == joinCondition.isJoinByKey
        && isAntiJoin == joinCondition.isAntiJoin;
  }

  @Override
  public int hashCode() {
    return Objects.hash(joinType, filter, joinColumns, isJoinByKey, isAntiJoin);
  }
}
