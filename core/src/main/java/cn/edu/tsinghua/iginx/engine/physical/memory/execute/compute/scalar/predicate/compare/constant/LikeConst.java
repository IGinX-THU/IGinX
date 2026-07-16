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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare.Like;
import java.util.regex.Pattern;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class LikeConst extends CompareConst {

  public final Pattern pattern;

  public LikeConst(byte[] pattern) {
    super(Like.NAME, pattern);
    this.pattern = Pattern.compile(new String(pattern));
  }

  @Override
  protected boolean evaluateImpl(boolean value) {
    return false;
  }

  @Override
  protected boolean evaluateImpl(int value) {
    return false;
  }

  @Override
  protected boolean evaluateImpl(long value) {
    return false;
  }

  @Override
  protected boolean evaluateImpl(float value) {
    return false;
  }

  @Override
  protected boolean evaluateImpl(double value) {
    return false;
  }

  @Override
  protected boolean evaluateImpl(ArrowBufPointer value) {
    byte[] inputBytes = new byte[(int) value.getLength()];
    ArrowBuf inputBuf = value.getBuf();
    if (inputBuf != null) {
      inputBuf.getBytes(value.getOffset(), inputBytes);
    }
    String targetString = new String(inputBytes);
    return pattern.matcher(targetString).matches();
  }
}
