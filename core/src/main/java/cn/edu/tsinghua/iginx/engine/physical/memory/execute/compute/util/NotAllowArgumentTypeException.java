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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import org.apache.arrow.vector.types.Types;

public class NotAllowArgumentTypeException extends ArgumentException {

  private final int index;
  private final Types.MinorType type;

  public NotAllowArgumentTypeException(PhysicalFunction function, int index, Types.MinorType type) {
    super(function, "not allow type " + type + " at index " + index);
    this.index = index;
    this.type = type;
  }

  public int getIndex() {
    return index;
  }

  public Types.MinorType getType() {
    return type;
  }
}
