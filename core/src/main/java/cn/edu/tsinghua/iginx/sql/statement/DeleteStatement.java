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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends DataStatement {

  private boolean deleteAll; // delete data & path

  private List<String> paths;
  private List<KeyRange> keyRanges;
  private TagFilter tagFilter;

  private boolean involveDummyData;
  private boolean noWritableFragment; // no writable engine, deletion will not be executed.

  public DeleteStatement() {
    this.statementType = StatementType.DELETE;
    this.paths = new ArrayList<>();
    this.keyRanges = new ArrayList<>();
    this.deleteAll = false;
    this.tagFilter = null;
    this.involveDummyData = false;
    this.noWritableFragment = false;
  }

  public DeleteStatement(List<String> paths, long startKey, long endKey) {
    this.statementType = StatementType.DELETE;
    this.paths = paths;
    this.keyRanges = new ArrayList<>();
    this.keyRanges.add(new KeyRange(startKey, endKey));
    this.deleteAll = false;
    this.tagFilter = null;
    this.involveDummyData = false;
    this.noWritableFragment = false;
  }

  public DeleteStatement(List<String> paths) {
    this(paths, null);
  }

  public DeleteStatement(List<String> paths, TagFilter tagFilter) {
    this.statementType = StatementType.DELETE;
    this.paths = paths;
    this.keyRanges = new ArrayList<>();
    this.deleteAll = true;
    this.tagFilter = tagFilter;
    this.involveDummyData = false;
    this.noWritableFragment = false;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void addPath(String path) {
    paths.add(path);
  }

  public List<KeyRange> getKeyRanges() {
    return keyRanges;
  }

  public void setKeyRanges(List<KeyRange> keyRanges) {
    this.keyRanges = keyRanges;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  public boolean isInvolveDummyData() {
    return involveDummyData;
  }

  public void setInvolveDummyData(boolean involveDummyData) {
    this.involveDummyData = involveDummyData;
  }

  public boolean isNoWritableFragment() {
    return noWritableFragment;
  }

  public void setNoWritableFragment(boolean noWritableFragment) {
    this.noWritableFragment = noWritableFragment;
  }

  public void setKeyRangesByFilter(Filter filter) {
    if (filter != null) {
      this.keyRanges = LogicalFilterUtils.getKeyRangesFromFilter(filter);
      if (keyRanges.isEmpty()) {
        throw new SQLParserException("This clause delete nothing, check your filter again.");
      }
    }
  }

  public boolean isDeleteAll() {
    return deleteAll;
  }

  public void setDeleteAll(boolean deleteAll) {
    this.deleteAll = deleteAll;
  }
}
