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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import java.util.ArrayList;
import java.util.List;

public class DeleteColumnsStatement extends DataStatement {

  private final List<String> paths;

  private TagFilter tagFilter;

  public DeleteColumnsStatement() {
    this.statementType = StatementType.DELETE_COLUMNS;
    this.paths = new ArrayList<>();
    this.tagFilter = null;
  }

  public DeleteColumnsStatement(List<String> paths) {
    this.statementType = StatementType.DELETE_COLUMNS;
    this.paths = paths;
    this.tagFilter = null;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void addPath(String path) {
    this.paths.add(path);
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }
}
