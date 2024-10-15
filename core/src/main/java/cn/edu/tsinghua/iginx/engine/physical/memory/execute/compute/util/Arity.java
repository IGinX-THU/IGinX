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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

public enum Arity {
  NULLARY(0, false),
  UNARY(1, false),
  BINARY(2, false),
  TERNARY(3, false),
  VAR_ARGS(0, true);

  private final int arity;
  public final boolean varArgs;

  Arity(int arity, boolean varArgs) {
    this.arity = arity;
    this.varArgs = varArgs;
  }

  public int getArity() {
    return arity;
  }

  public boolean isVarArgs() {
    return varArgs;
  }

  public boolean checkArity(int arity) {
    return this.arity == arity || (varArgs && arity >= this.arity);
  }
}
