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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.NotAllowTypeException;
import java.util.function.Predicate;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

public class CallContracts {
  private CallContracts() {}

  public static void ensureType(PhysicalFunction function, Schema schema, Types.MinorType type)
      throws NotAllowTypeException {
    ensureType(function, schema, type::equals);
  }

  public static void ensureType(
      PhysicalFunction function, Schema schema, Predicate<Types.MinorType> predicate)
      throws NotAllowTypeException {
    for (int i = 0; i < schema.getFields().size(); i++) {
      Types.MinorType minorType =
          Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
      if (!predicate.test(minorType)) {
        throw new NotAllowTypeException(function, schema, i);
      }
    }
  }
}
