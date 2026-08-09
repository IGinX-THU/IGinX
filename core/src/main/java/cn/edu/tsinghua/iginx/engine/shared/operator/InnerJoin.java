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

  private final boolean isJoinByKey;

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
        false,
        joinAlgType,
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
      boolean isJoinByKey,
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
        isJoinByKey,
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
      boolean isJoinByKey,
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
    this.isJoinByKey = isJoinByKey;
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
      boolean isJoinByKey,
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
        isJoinByKey,
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

  public boolean isJoinByKey() {
    return isJoinByKey;
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
    return copyWithSource(getSourceA().copy(), getSourceB().copy());
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new InnerJoin(
        sourceA,
        sourceB,
        getPrefixA(),
        getPrefixB(),
        filter == null ? null : filter.copy(),
        tagFilter == null ? null : tagFilter.copy(),
        new ArrayList<>(joinColumns),
        isNaturalJoin,
        isJoinByKey,
        getJoinAlgType(),
        new ArrayList<>(getExtraJoinPrefix()));
  }

  @Override
  public String getInfo() {
    // TODO
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
        && isJoinByKey == that.isJoinByKey
        && joinColumns.equals(that.joinColumns)
        && filter.equals(that.filter)
        && tagFilter.equals(that.tagFilter)
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
