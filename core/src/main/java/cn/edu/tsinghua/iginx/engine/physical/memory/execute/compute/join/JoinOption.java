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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join;

import lombok.Getter;

@Getter
public enum JoinOption {
  INNER(false, false),
  LEFT(true, false),
  RIGHT(false, true),
  FULL(true, true),
  MARK(false, true, true, true, false, true),
  SINGLE(false, true, true, false, false, false);

  private final boolean toOutputProbeSideUnmatched;
  private final boolean toOutputBuildSideUnmatched;
  private final boolean orderByProbeSideOrdinal;
  private final boolean allowedToMatchMultiple;
  private final boolean toOutputAllMatched;
  private final boolean toOutputMark;

  JoinOption(boolean toOutputBuildSideUnmatched, boolean toOutputProbeSideUnmatched) {
    this(toOutputBuildSideUnmatched, toOutputProbeSideUnmatched, false, true, true, false);
  }

  JoinOption(
      boolean toOutputBuildSideUnmatched,
      boolean toOutputProbeSideUnmatched,
      boolean orderByProbeSideOrdinal,
      boolean allowedToMatchMultiple,
      boolean toOutputAllMatched,
      boolean toOutputMark) {
    this.toOutputBuildSideUnmatched = toOutputBuildSideUnmatched;
    this.toOutputProbeSideUnmatched = toOutputProbeSideUnmatched;
    this.orderByProbeSideOrdinal = orderByProbeSideOrdinal;
    this.allowedToMatchMultiple = allowedToMatchMultiple;
    this.toOutputAllMatched = toOutputAllMatched;
    this.toOutputMark = toOutputMark;
  }
}
