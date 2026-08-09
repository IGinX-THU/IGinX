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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare;

import java.util.regex.Pattern;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class Like extends BinaryComparisonFunction {

  public static final String NAME = "like";

  public Like() {
    super(NAME);
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
    byte[] leftBytes = new byte[(int) left.getLength()];
    ArrowBuf leftBuf = left.getBuf();
    if (leftBuf != null) {
      leftBuf.getBytes(left.getOffset(), leftBytes);
    }
    String targetString = new String(leftBytes);

    byte[] rightBytes = new byte[(int) right.getLength()];
    ArrowBuf rightBuf = right.getBuf();
    if (rightBuf != null) {
      rightBuf.getBytes(right.getOffset(), rightBytes);
    }
    String pattern = new String(rightBytes);
    return Pattern.matches(pattern, targetString);
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass().equals(this.getClass());
  }
}
