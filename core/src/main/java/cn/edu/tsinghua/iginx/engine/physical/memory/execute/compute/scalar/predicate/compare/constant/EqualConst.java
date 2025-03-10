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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.Equal;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class EqualConst extends CompareConst {

  public EqualConst(Object value) {
    this(Equal.NAME, value);
  }

  protected EqualConst(String name, Object value) {
    super(name, value);
  }

  @Override
  protected boolean evaluateImpl(boolean value) {
    return value == bValue;
  }

  @Override
  protected boolean evaluateImpl(int value) {
    return value == iValue;
  }

  @Override
  protected boolean evaluateImpl(long value) {
    return value == lValue;
  }

  @Override
  protected boolean evaluateImpl(float value) {
    return value == fValue;
  }

  @Override
  protected boolean evaluateImpl(double value) {
    return value == dValue;
  }

  @Override
  protected boolean evaluateImpl(ArrowBufPointer value) {
    if (value.getLength() != bytes.length) {
      return false;
    }
    if (bytes.length == 0) {
      return true;
    }
    ArrowBuf buf = value.getBuf();
    assert buf != null;
    long offset = value.getOffset();
    for (int i = 0; i < bytes.length; i++) {
      if (bytes[i] != buf.getByte(offset + i)) {
        return false;
      }
    }
    return true;
  }
}
