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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare;

import java.util.Arrays;
import java.util.Objects;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class EqualConstBinary extends UnaryComparisonFunction {

  private final byte[] bytes;

  public EqualConstBinary(byte[] value) {
    super("equal<" + new String(value) + ">");
    this.bytes = Objects.requireNonNull(value);
  }

  @Override
  protected boolean evaluate(int input) {
    return false;
  }

  @Override
  protected boolean evaluate(long input) {
    return false;
  }

  @Override
  protected boolean evaluate(float input) {
    return false;
  }

  @Override
  protected boolean evaluate(double input) {
    return false;
  }

  @Override
  protected boolean evaluate(ArrowBufPointer input) {
    if (input.getLength() != bytes.length) {
      return false;
    }
    if (bytes.length > 0) {
      byte firstByte = input.getBuf().getByte(input.getOffset());
      if (firstByte != bytes[0]) {
        return false;
      }
      if (bytes.length > 1) {
        byte lastByte = input.getBuf().getByte(input.getOffset() + bytes.length - 1);
        if (lastByte != bytes[bytes.length - 1]) {
          return false;
        }
      }
    }
    byte[] constValue = new byte[bytes.length];
    input.getBuf().getBytes(input.getOffset(), constValue);
    return Arrays.equals(bytes, constValue);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EqualConstBinary that = (EqualConstBinary) o;
    return Objects.deepEquals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), Arrays.hashCode(bytes));
  }
}
