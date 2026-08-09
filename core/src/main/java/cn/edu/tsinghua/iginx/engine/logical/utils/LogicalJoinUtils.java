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
package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;

public class LogicalJoinUtils {
  private LogicalJoinUtils() {}

  public static CrossJoin reverse(CrossJoin operator) {
    return new CrossJoin(
        operator.getSourceB(),
        operator.getSourceA(),
        operator.getPrefixB(),
        operator.getPrefixA(),
        operator.getExtraJoinPrefix());
  }

  public static SingleJoin reverse(SingleJoin join) {
    return new SingleJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getFilter(),
        join.getTagFilter(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix());
  }

  public static MarkJoin reverse(MarkJoin join) {
    return new MarkJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getFilter(),
        join.getTagFilter(),
        join.getMarkColumn(),
        join.isAntiJoin(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix());
  }

  public static InnerJoin reverse(InnerJoin join) {
    return new InnerJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getPrefixB(),
        join.getPrefixA(),
        join.getFilter(),
        join.getTagFilter(),
        join.getJoinColumns(),
        join.isNaturalJoin(),
        join.isJoinByKey(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix());
  }

  public static OuterJoin reverse(OuterJoin join) {
    return new OuterJoin(
        join.getSourceB(),
        join.getSourceA(),
        join.getPrefixB(),
        join.getPrefixA(),
        reverse(join.getOuterJoinType()),
        join.getFilter(),
        join.getJoinColumns(),
        join.isNaturalJoin(),
        join.isJoinByKey(),
        join.getJoinAlgType(),
        join.getExtraJoinPrefix());
  }

  public static OuterJoinType reverse(OuterJoinType type) {
    switch (type) {
      case LEFT:
        return OuterJoinType.RIGHT;
      case RIGHT:
        return OuterJoinType.LEFT;
      default:
        return type;
    }
  }
}
