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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class OuterJoin extends AbstractJoin {

  private OuterJoinType outerJoinType;

  private Filter filter;

  private final List<String> joinColumns;

  private final boolean isNaturalJoin;

  private final boolean isJoinByKey;

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
      Filter filter,
      List<String> joinColumns) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        outerJoinType,
        filter,
        joinColumns,
        false,
        false,
        JoinAlgType.HashJoin,
        new ArrayList<>());
  }

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      JoinAlgType joinAlgType) {
    this(
        sourceA,
        sourceB,
        prefixA,
        prefixB,
        outerJoinType,
        filter,
        joinColumns,
        isNaturalJoin,
        false,
        joinAlgType,
        new ArrayList<>());
  }

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
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
        outerJoinType,
        filter,
        joinColumns,
        isNaturalJoin,
        isJoinByKey,
        joinAlgType,
        new ArrayList<>());
  }

  public OuterJoin(
      Source sourceA,
      Source sourceB,
      String prefixA,
      String prefixB,
      OuterJoinType outerJoinType,
      Filter filter,
      List<String> joinColumns,
      boolean isNaturalJoin,
      boolean isJoinByKey,
      JoinAlgType joinAlgType,
      List<String> extraJoinPrefix) {
    super(OperatorType.OuterJoin, sourceA, sourceB, prefixA, prefixB, joinAlgType, extraJoinPrefix);
    this.outerJoinType = outerJoinType;
    this.filter = filter;
    if (joinColumns != null) {
      this.joinColumns = joinColumns;
    } else {
      this.joinColumns = new ArrayList<>();
    }
    this.isNaturalJoin = isNaturalJoin;
    this.isJoinByKey = isJoinByKey;
  }

  public OuterJoinType getOuterJoinType() {
    return outerJoinType;
  }

  public void setOuterJoinType(OuterJoinType outerJoinType) {
    this.outerJoinType = outerJoinType;
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

  public void reChooseJoinAlg() {
    setJoinAlgType(
        JoinAlgType.chooseJoinAlg(filter, isNaturalJoin, joinColumns, getExtraJoinPrefix()));
  }

  @Override
  public Operator copy() {
    return copyWithSource(getSourceA().copy(), getSourceB().copy());
  }

  @Override
  public BinaryOperator copyWithSource(Source sourceA, Source sourceB) {
    return new OuterJoin(
        sourceA,
        sourceB,
        getPrefixA(),
        getPrefixB(),
        outerJoinType,
        filter == null ? null : filter.copy(),
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
    builder.append(", OuterJoinType: ").append(outerJoinType);
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
    OuterJoin that = (OuterJoin) object;
    return outerJoinType == that.outerJoinType
        && filter.equals(that.filter)
        && joinColumns.equals(that.joinColumns)
        && isNaturalJoin == that.isNaturalJoin
        && isJoinByKey == that.isJoinByKey
        && getExtraJoinPrefix().equals(that.getExtraJoinPrefix());
  }
}
