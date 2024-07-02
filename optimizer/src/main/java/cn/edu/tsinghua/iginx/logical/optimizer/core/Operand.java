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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.logical.optimizer.core;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;

public class Operand {

  private final Class<? extends Operator> clazz;

  private final List<Operand> children;

  public static final Operand ANY_OPERAND = new Operand(null, null);

  public Operand(Class<? extends Operator> clazz, List<Operand> children) {
    this.clazz = clazz;
    this.children = children;
  }

  public List<Operand> getChildren() {
    return children;
  }

  public boolean matches(Operator operator) {
    if (this == ANY_OPERAND) {
      return true;
    }
    return clazz.isInstance(operator);
  }
}
