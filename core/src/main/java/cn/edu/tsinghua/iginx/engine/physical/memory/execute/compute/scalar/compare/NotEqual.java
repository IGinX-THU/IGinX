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

import org.apache.arrow.memory.util.ArrowBufPointer;

public class NotEqual extends ComparisonFunction {

  private static final String name = "NotEqual";

  public NotEqual() {
    super(name);
  }

  @Override
  protected boolean evaluate(int left, int right) {
    return left != right;
  }

  @Override
  protected boolean evaluate(long left, long right) {
    return left != right;
  }

  @Override
  protected boolean evaluate(float left, float right) {
    return left != right;
  }

  @Override
  protected boolean evaluate(double left, double right) {
    return left != right;
  }

  @Override
  protected boolean evaluate(ArrowBufPointer left, ArrowBufPointer right) {
    return left.compareTo(right) != 0;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof NotEqual;
  }
}
