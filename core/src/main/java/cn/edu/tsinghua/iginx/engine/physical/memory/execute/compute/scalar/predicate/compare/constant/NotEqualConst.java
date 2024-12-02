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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.NotEqual;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class NotEqualConst extends EqualConst {

  public NotEqualConst(Object value) {
    super(NotEqual.NAME, value);
  }

  @Override
  protected boolean evaluateImpl(boolean value) {
    return !super.evaluateImpl(value);
  }

  @Override
  protected boolean evaluateImpl(int value) {
    return !super.evaluateImpl(value);
  }

  @Override
  protected boolean evaluateImpl(long value) {
    return !super.evaluateImpl(value);
  }

  @Override
  protected boolean evaluateImpl(float value) {
    return !super.evaluateImpl(value);
  }

  @Override
  protected boolean evaluateImpl(double value) {
    return !super.evaluateImpl(value);
  }

  @Override
  protected boolean evaluateImpl(ArrowBufPointer value) {
    return !super.evaluateImpl(value);
  }
}
