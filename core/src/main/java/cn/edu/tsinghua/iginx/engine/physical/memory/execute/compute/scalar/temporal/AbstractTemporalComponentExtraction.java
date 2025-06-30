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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.temporal;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.convert.cast.CastAsBigInt;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;

public abstract class AbstractTemporalComponentExtraction extends UnaryScalarFunction<IntVector> {

  protected AbstractTemporalComponentExtraction(String name) {
    super(name);
  }

  @Override
  public IntVector evaluate(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector input)
      throws ComputeException {
    try (BigIntVector bigIntVector = new CastAsBigInt().evaluate(allocator, selection, input)) {
      return evaluate(allocator, bigIntVector);
    }
  }

  public IntVector evaluate(BufferAllocator allocator, BigIntVector input) throws ComputeException {
    IntVector result = new IntVector(getName() + "(" + input.getName() + ")", allocator);
    result.allocateNew();
    result.setValueCount(input.getValueCount());
    for (int i = 0; i < input.getValueCount(); i++) {
      if (input.isNull(i)) {
        result.setNull(i);
        continue;
      }
      long timestamp = input.get(i);
      Instant instant = Instant.ofEpochMilli(timestamp);
      LocalDateTime localDateTime =
          instant.atZone(TimeZone.getTimeZone("GMT").toZoneId()).toLocalDateTime();
      int component = getTemporalComponent(localDateTime);
      result.set(i, component);
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    return obj != null && obj.getClass() == getClass();
  }

  protected abstract int getTemporalComponent(LocalDateTime dateTime);
}
