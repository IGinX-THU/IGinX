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

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;

@ThreadSafe
public class ConstantPool implements AutoCloseable {

  private final BufferAllocator allocator;

  private final ConcurrentHashMap<ConstantValue, FieldVector> pool = new ConcurrentHashMap<>();

  public ConstantPool(BufferAllocator allocator) {
    Preconditions.checkNotNull(allocator, "allocator is null");
    this.allocator = allocator;
  }

  public FieldVector getConstantVector(BufferAllocator allocator, Object value, int size) {
    Preconditions.checkArgument(size >= 0, "size must be non-negative");
    if (size == 0) {
      return ConstantVectors.of(allocator, value, 0);
    }

    FieldVector[] result = new FieldVector[1];
    pool.compute(
        new ConstantValue(value),
        (key, existingValue) -> {
          if (existingValue == null) {
            existingValue = ConstantVectors.of(this.allocator, value, size);
          } else if (existingValue.getValueCount() < size) {
            existingValue.close();
            existingValue = ConstantVectors.of(this.allocator, value, size);
          }

          result[0] = ValueVectors.slice(allocator, existingValue, size);
          return existingValue;
        });
    return result[0];
  }

  @Override
  public synchronized void close() {
    pool.values().forEach(FieldVector::close);
    pool.clear();
  }

  private static class ConstantValue {

    private final Object value;
    private final int hashCode;

    private ConstantValue(Object value) {
      this.value = value;
      this.hashCode = Arrays.deepHashCode(new Object[] {value});
    }

    private Object getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConstantValue that = (ConstantValue) o;
      return Objects.deepEquals(value, that.value);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
