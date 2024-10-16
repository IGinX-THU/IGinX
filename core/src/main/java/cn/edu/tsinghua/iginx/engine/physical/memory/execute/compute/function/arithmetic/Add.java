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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.arithmetic;

public class Add extends BinaryArithmeticFunction {

  public Add() {
    super("add");
  }

  @Override
  public int evaluate(int left, int right) {
    return left + right;
  }

  @Override
  public long evaluate(long left, long right) {
    return left + right;
  }

  @Override
  public float evaluate(float left, float right) {
    return left + right;
  }

  @Override
  public double evaluate(double left, double right) {
    return left + right;
  }
}
