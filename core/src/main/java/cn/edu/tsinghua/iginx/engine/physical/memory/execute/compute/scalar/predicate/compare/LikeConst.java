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

import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class LikeConst extends UnaryComparisonFunction {

  public final Pattern pattern;

  public LikeConst(byte[] pattern) {
    this(new String(pattern));
  }

  private LikeConst(String pattern) {
    this("like<\"" + pattern + "\">", pattern);
  }

  protected LikeConst(String name, String pattern) {
    super(name);
    this.pattern = Pattern.compile(pattern);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LikeConst likeConst = (LikeConst) o;
    return Objects.equals(pattern, likeConst.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), pattern);
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
    byte[] inputBytes = new byte[(int) input.getLength()];
    ArrowBuf inputBuf = input.getBuf();
    if (inputBuf != null) {
      inputBuf.getBytes(input.getOffset(), inputBytes);
    }
    String targetString = new String(inputBytes);
    return pattern.matcher(targetString).matches();
  }
}
