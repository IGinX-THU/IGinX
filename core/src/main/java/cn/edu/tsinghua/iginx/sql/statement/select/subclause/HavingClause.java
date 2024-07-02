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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HavingClause {
  private Filter havingFilter;
  private final List<SubQueryFromPart> havingSubQueryParts;

  private final Set<String> pathSet;

  public HavingClause() {
    this.havingSubQueryParts = new ArrayList<>();
    this.pathSet = new HashSet<>();
  }

  public Filter getHavingFilter() {
    return havingFilter;
  }

  public void setHavingFilter(Filter havingFilter) {
    this.havingFilter = havingFilter;
  }

  public List<SubQueryFromPart> getHavingSubQueryParts() {
    return havingSubQueryParts;
  }

  public void addHavingSubQueryPart(SubQueryFromPart subQueryFromPart) {
    havingSubQueryParts.add(subQueryFromPart);
  }

  public boolean hasSubQuery() {
    return !havingSubQueryParts.isEmpty();
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }
}
