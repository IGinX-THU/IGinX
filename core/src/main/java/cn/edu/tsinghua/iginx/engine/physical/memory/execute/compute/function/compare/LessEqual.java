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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare;

public class LessEqual extends ComparisonFunction {

  public static final String NAME = "less_equal";

  public LessEqual() {
    super(NAME);
  }

  @Override
  public boolean evaluate(int left, int right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(long left, long right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(float left, float right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(double left, double right) {
    return left <= right;
  }

  @Override
  public boolean evaluate(byte[] left, byte[] right) {
    return new String(left).compareTo(new String(right)) <= 0;
  }
}
