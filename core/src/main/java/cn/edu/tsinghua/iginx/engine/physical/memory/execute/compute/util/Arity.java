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

import org.apache.arrow.util.Preconditions;

public class Arity {

  public static Arity UNARY = new Arity(1);
  public static Arity BINARY = new Arity(2);

  private final int arity;
  private final boolean varArgs;

  public Arity(int arity) {
    this(arity, false);
  }

  public Arity(int arity, boolean varArgs) {
    Preconditions.checkArgument(arity >= 0, "Arity must be non-negative");
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
