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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.constant;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.UnaryComparisonFunction;
import java.util.Arrays;
import org.apache.arrow.memory.util.ArrowBufPointer;

public abstract class CompareConst extends UnaryComparisonFunction {

  private final boolean isBoolean;
  private final boolean isInt;
  private final boolean isLong;
  private final boolean isFloat;
  private final boolean isNumeric;
  private final boolean isBytes;
  protected final boolean bValue;
  protected final int iValue;
  protected final long lValue;
  protected final float fValue;
  protected final double dValue;
  protected final byte[] bytes;

  protected CompareConst(String name, Object value) {
    super(
        name
            + "<"
            + (value instanceof byte[]
                ? "\"" + new String((byte[]) value) + "\""
                : String.valueOf(value))
            + ">");
    boolean isBoolean = false;
    boolean isInt = false;
    boolean isLong = false;
    boolean isFloat = false;
    boolean isNumeric = false;
    boolean isBytes = false;
    boolean bValue = false;
    int iValue = 0;
    long lValue = 0;
    float fValue = 0;
    double dValue = 0;
    byte[] bytes = null;
    if (value instanceof Boolean) {
      isBoolean = true;
      bValue = (Boolean) value;
    } else if (value instanceof Integer) {
      isInt = true;
      isNumeric = true;
      iValue = (Integer) value;
      dValue = iValue;
    } else if (value instanceof Long) {
      isLong = true;
      isNumeric = true;
      lValue = (Long) value;
      dValue = lValue;
    } else if (value instanceof Float) {
      isFloat = true;
      isNumeric = true;
      fValue = (Float) value;
      dValue = fValue;
    } else if (value instanceof Double) {
      isNumeric = true;
      dValue = (Double) value;
    } else if (value instanceof byte[]) {
      isBytes = true;
      bytes = (byte[]) value;
    } else {
      throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getName());
    }
    this.isBoolean = isBoolean;
    this.isInt = isInt;
    this.isLong = isLong;
    this.isFloat = isFloat;
    this.isNumeric = isNumeric;
    this.isBytes = isBytes;
    this.bValue = bValue;
    this.iValue = iValue;
    this.lValue = lValue;
    this.fValue = fValue;
    this.dValue = dValue;
    this.bytes = bytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompareConst that = (CompareConst) o;
    return Arrays.deepEquals(
        new Object[] {getName(), bValue, iValue, lValue, fValue, dValue, bytes},
        new Object[] {
          that.getName(),
          that.bValue,
          that.iValue,
          that.lValue,
          that.fValue,
          that.dValue,
          that.bytes
        });
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(
        new Object[] {getName(), bValue, iValue, lValue, fValue, dValue, bytes});
  }

  @Override
  protected boolean evaluate(boolean input) {
    return isBoolean && evaluateImpl(input);
  }

  @Override
  protected boolean evaluate(int input) {
    return isInt && evaluateImpl(input) || isNumeric && evaluateImpl((double) input);
  }

  @Override
  protected boolean evaluate(long input) {
    return isLong && evaluateImpl(input) || isNumeric && evaluateImpl((double) input);
  }

  @Override
  protected boolean evaluate(float input) {
    return isFloat && evaluateImpl(input) || isNumeric && evaluateImpl((double) input);
  }

  @Override
  protected boolean evaluate(double input) {
    return isNumeric && evaluateImpl(input);
  }

  @Override
  protected boolean evaluate(ArrowBufPointer input) {
    return isBytes && evaluateImpl(input);
  }

  protected abstract boolean evaluateImpl(boolean value);

  protected abstract boolean evaluateImpl(int value);

  protected abstract boolean evaluateImpl(long value);

  protected abstract boolean evaluateImpl(float value);

  protected abstract boolean evaluateImpl(double value);

  protected abstract boolean evaluateImpl(ArrowBufPointer value);
}
