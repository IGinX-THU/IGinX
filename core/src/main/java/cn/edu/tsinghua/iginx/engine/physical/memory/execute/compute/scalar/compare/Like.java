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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.compare;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class Like extends ComparisonFunction {

  public Like() {
    super("like");
  }

  protected Like(String name) {
    super(name);
  }

  @Override
  protected boolean evaluate(int left, int right) {
    throw new UnsupportedOperationException("LIKE can only be evaluated between binary vectors.");
  }

  @Override
  protected boolean evaluate(long left, long right) {
    throw new UnsupportedOperationException("LIKE can only be evaluated between binary vectors.");
  }

  @Override
  protected boolean evaluate(float left, float right) {
    throw new UnsupportedOperationException("LIKE can only be evaluated between binary vectors.");
  }

  @Override
  protected boolean evaluate(double left, double right) {
    throw new UnsupportedOperationException("LIKE can only be evaluated between binary vectors.");
  }

  @Override
  public boolean evaluate(ArrowBufPointer left, ArrowBufPointer right) {
    ArrowBuf leftBuf = left.getBuf();
    ArrowBuf rightBuf = right.getBuf();
    if (leftBuf == null || rightBuf == null) {
      return false;
    }
    int leftLen = (int) left.getLength();
    int rightLen = (int) right.getLength();
    if (leftLen == 0 || rightLen == 0) {
      return false;
    }
    byte[] leftUtf8Bytes = new byte[leftLen];
    left.getBuf().getBytes(left.getOffset(), leftUtf8Bytes);
    String targetString = new String(leftUtf8Bytes, StandardCharsets.UTF_8);

    byte[] rightUtf8Bytes = new byte[rightLen];
    right.getBuf().getBytes(right.getOffset(), rightUtf8Bytes);
    String pattern = new String(rightUtf8Bytes, StandardCharsets.UTF_8);
    return Pattern.matches(pattern, targetString);
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass().equals(this.getClass());
  }
}
