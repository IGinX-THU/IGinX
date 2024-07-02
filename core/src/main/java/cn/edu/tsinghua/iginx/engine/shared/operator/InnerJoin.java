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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import static cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType.chooseJoinAlg;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class InnerJoin extends AbstractJoin {

  private Filter filter;

  private TagFilter tagFilter;

  private final List<String> joinColumns;

  private final boolean isNaturalJoin;

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns) {
    this(sourceA, sourceB, prefixA, prefixB, filter, joinColumns, false);
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      TagFilter tagFilter,
      List<String> joinColumns) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        filter,
        tagFilter,
        joinColumns,
        false,
        JoinAlgType.HashJoin,
        new ArrayList<>());
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        filter,
        null,
        joinColumns,
        isNaturalJoin,
        JoinAlgType.HashJoin,
        new ArrayList<>());
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        filter,
        null,
        joinColumns,
        isNaturalJoin,
        joinAlgType,
        new ArrayList<>());
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      TagFilter tagFilter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(OperatorType.InnerJoin, sourceA, sourceB, prefixA, prefixB, joinAlgType, extraJoinPrefix);
    this.filter = filter;
    if (joinColumns != null) {
      this.joinColumns = joinColumns;
    } else {
      this.joinColumns = new ArrayList<>();
    }
    this.isNaturalJoin = isNaturalJoin;
    this.tagFilter = tagFilter;
  }

  public InnerJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        filter,
        null,
        joinColumns,
        isNaturalJoin,
        joinAlgType,
        extraJoinPrefix);
  }

  public Filter getFilter() {
    return filter;
  }

  public List<String> getJoinColumns() {
    return joinColumns;
  }

  public boolean isNaturalJoin() {
    return isNaturalJoin;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }

  public void reChooseJoinAlg() {
    setJoinAlgType(chooseJoinAlg(filter, isNaturalJoin, joinColumns, getExtraJoinPrefix()));
  }

  @Override
  public Operator copy() {
    return new InnerJoin(
        getSourceA().copy(),
        getSourceB().copy(),
        getPrefixA(),
        getPrefixB(),
        filter.copy(),
        tagFilter.copy(),
        new ArrayList<>(joinColumns),
        isNaturalJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new InnerJoin(
        sourceA,
        sourceB,
        getPrefixA(),
        getPrefixB(),
        filter.copy(),
        tagFilter.copy(),
        new ArrayList<>(joinColumns),
        isNaturalJoin,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("PrefixA: ").append(getPrefixA());
    builder.append(", PrefixB: ").append(getPrefixB());
    builder.append(", IsNatural: ").append(isNaturalJoin);
    if (filter != null) {
      builder.append(", Filter: ").append(filter);
    }
    if (joinColumns != null && !joinColumns.isEmpty()) {
      builder.append(", JoinColumns: ");
      for (String col : joinColumns) {
        builder.append(col).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    if (getExtraJoinPrefix() != null && !getExtraJoinPrefix().isEmpty()) {
      builder.append(", ExtraJoinPrefix: ");
      for (String col : getExtraJoinPrefix()) {
        builder.append(col).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    InnerJoin that = (InnerJoin) object;
    return isNaturalJoin == that.isNaturalJoin
        && joinColumns.equals(that.joinColumns)
        && filter.equals(that.filter)
        && tagFilter.equals(that.tagFilter)
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
