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

import org.apache.arrow.memory.util.ArrowBufPointer;

public class Greater extends BinaryComparisonFunction {

  public Greater() {
    super("greater");
  }

  @Override
  protected boolean evaluate(int left, int right) {
    return left > right;
  }

  @Override
  protected boolean evaluate(long left, long right) {
    return left > right;
  }

  @Override
  protected boolean evaluate(float left, float right) {
    return left > right;
  }

  @Override
  protected boolean evaluate(double left, double right) {
    return left > right;
  }

  @Override
  protected boolean evaluate(ArrowBufPointer left, ArrowBufPointer right) {
    return left.compareTo(right) > 0;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Greater;
  }
}
