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

import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement.QueryType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GroupByClause {
  private boolean hasDownsample;
  private boolean hasGroupBy;
  private QueryType queryType;
  private final List<String> groupByPaths;
  private final Set<String> pathSet;
  private long precision;
  private long slideDistance;

  public GroupByClause() {
    groupByPaths = new ArrayList<>();
    pathSet = new HashSet<>();
    hasDownsample = false;
    hasGroupBy = false;
    queryType = QueryType.Unknown;
  }

  public void addGroupByPath(String path) {
    groupByPaths.add(path);
    hasGroupBy = true;
  }

  public List<String> getGroupByPaths() {
    return groupByPaths;
  }

  public boolean hasDownsample() {
    return hasDownsample;
  }

  public void setHasDownsample(boolean hasDownsample) {
    this.hasDownsample = hasDownsample;
  }

  public boolean hasGroupBy() {
    return hasGroupBy;
  }

  public void setHasGroupBy(boolean hasGroupBy) {
    this.hasGroupBy = hasGroupBy;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public Set<String> getPathSet() {
    return pathSet;
  }

  public void addPath(String path) {
    pathSet.add(path);
  }

  public long getPrecision() {
    return precision;
  }

  public void setPrecision(long precision) {
    this.precision = precision;
  }

  public long getSlideDistance() {
    return slideDistance;
  }

  public void setSlideDistance(long slideDistance) {
    this.slideDistance = slideDistance;
  }
}
