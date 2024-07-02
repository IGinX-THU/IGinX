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
  private final String markColumn;
  private final boolean isAntiJoin;

  public JoinCondition() {
    this(JoinType.CrossJoin, null, Collections.emptyList());
  }

  public JoinCondition(JoinType joinType, Filter filter) {
    this(joinType, filter, Collections.emptyList());
  }

  public JoinCondition(JoinType joinType, Filter filter, List<String> joinColumns) {
    this.joinType = joinType;
    this.filter = filter;
    this.joinColumns = joinColumns;
    this.markColumn = null;
    this.isAntiJoin = false;
  }

  public JoinCondition(JoinType joinType, Filter filter, String markColumn, boolean isAntiJoin) {
    this.joinType = joinType;
    this.filter = filter;
    this.joinColumns = new ArrayList<>();
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
        && Objects.equals(joinColumns, joinCondition.joinColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(joinType, filter, joinColumns);
  }
}
