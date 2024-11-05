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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join;

import org.apache.arrow.util.Preconditions;

public class JoinOption {

  public static final JoinOption INNER = new JoinOption(false, false);
  public static final JoinOption LEFT = new JoinOption(true, false);
  public static final JoinOption RIGHT = new JoinOption(false, true);
  public static final JoinOption FULL = new JoinOption(true, true);

  private final boolean outputBuildSideUnmatched;
  private final boolean outputProbeSideUnmatched;
  private final boolean rightMatchMultipleLeft;
  private final boolean allMatched;
  private final String markColumn;
  private final boolean antiMark;

  private JoinOption(boolean outputBuildSideUnmatched, boolean outputProbeSideUnmatched) {
    this(outputBuildSideUnmatched, outputProbeSideUnmatched, true, true, null, false);
  }

  private JoinOption(
      boolean outputBuildSideUnmatched,
      boolean outputProbeSideUnmatched,
      boolean rightMatchMultipleLeft,
      boolean allMatched,
      String markColumn,
      boolean antiMark) {
    this.outputBuildSideUnmatched = outputBuildSideUnmatched;
    this.outputProbeSideUnmatched = outputProbeSideUnmatched;
    this.rightMatchMultipleLeft = rightMatchMultipleLeft;
    this.allMatched = allMatched;
    this.markColumn = markColumn;
    this.antiMark = antiMark;
  }

  public static JoinOption ofMark(String markColumn, boolean antiMark) {
    Preconditions.checkNotNull(markColumn);
    return new JoinOption(false, true, true, false, markColumn, antiMark);
  }

  public boolean needOutputBuildSideUnmatched() {
    return outputBuildSideUnmatched;
  }

  public boolean needOutputProbeSideUnmatched() {
    return outputProbeSideUnmatched;
  }

  public boolean allowProbeSideMatchMultiple() {
    return rightMatchMultipleLeft;
  }

  public boolean needAllMatched() {
    return allMatched;
  }

  public boolean needMark() {
    return markColumn != null;
  }

  public String markColumnName() {
    return markColumn;
  }

  public boolean isAntiMark() {
    return antiMark;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinOption joinOption = (JoinOption) o;
    return outputBuildSideUnmatched == joinOption.outputBuildSideUnmatched
        && outputProbeSideUnmatched == joinOption.outputProbeSideUnmatched
        && markColumn.equals(joinOption.markColumn);
  }

  @Override
  public String toString() {
    if (this.equals(INNER)) {
      return "INNER";
    } else if (this.equals(LEFT)) {
      return "LEFT";
    } else if (this.equals(RIGHT)) {
      return "RIGHT";
    } else if (this.equals(FULL)) {
      return "FULL";
    } else if (needMark()) {
      if (antiMark) {
        return "ANTI-MARK:" + markColumn;
      } else {
        return "MARK:" + markColumn;
      }
    }
    return "JoinType{"
        + "needOutputLeftUnmatched="
        + outputBuildSideUnmatched
        + ", needOutputRightUnmatched="
        + outputProbeSideUnmatched
        + ", markColumnName='"
        + markColumn
        + '\''
        + '}';
  }
}
