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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import org.apache.arrow.util.Preconditions;

public class Arity {

  public static Arity UNARY = new Arity(1);
  public static Arity BINARY = new Arity(2);
  public static Arity COMPLEX = new Arity(3);
  public static Arity ANY = new Arity(0, true);

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
