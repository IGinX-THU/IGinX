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
package cn.edu.tsinghua.iginx.engine.shared.operator.type;

public enum FuncType {
  // Exception [0, 10)
  Null(0),

  // Row2Row function [10, 20)
  Udtf(10),
  Ratio,

  // Set2Set function [20, 30)
  Udsf(20),
  First,
  Last,

  // Set2Row function [30, +∞)
  Udaf(30),
  Min,
  Max,
  Avg,
  Count,
  Sum,
  FirstValue,
  LastValue;

  private int value;

  FuncType() {
    this(FuncType.OperatorTypeCounter.nextValue);
  }

  FuncType(int value) {
    this.value = value;
    FuncType.OperatorTypeCounter.nextValue = value + 1;
  }

  public int getValue() {
    return value;
  }

  private static class OperatorTypeCounter {
    private static int nextValue = 0;
  }

  public static boolean isRow2RowFunc(FuncType type) {
    return type.value >= 10 && type.value < 20;
  }

  public static boolean isSet2SetFunc(FuncType type) {
    return type.value >= 20 && type.value < 30;
  }

  public static boolean isSet2RowFunc(FuncType type) {
    return type.value >= 30;
  }
}
