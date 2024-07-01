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

package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import java.util.HashSet;
import java.util.Set;

public class ShowColumnsStatement extends DataStatement {

  private Set<String> pathRegexSet;
  private TagFilter tagFilter;

  private int limit;
  private int offset;

  public ShowColumnsStatement() {
    this.statementType = StatementType.SHOW_COLUMNS;
    this.pathRegexSet = new HashSet<>();
    this.limit = Integer.MAX_VALUE;
    this.offset = 0;
  }

  public void setPathRegex(String pathRegex) {
    this.pathRegexSet.add(pathRegex);
  }

  public Set<String> getPathRegexSet() {
    return pathRegexSet;
  }

  public void setPathRegexSet(Set<String> pathRegexSet) {
    this.pathRegexSet = pathRegexSet;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
}
