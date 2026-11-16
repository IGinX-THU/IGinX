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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.BinaryHolder;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class InSet extends UnaryComparisonFunction {

  public static final String NAME_PREFIX = "in";
  public final HashSet<Object> values;

  public InSet(Set<Object> values) {
    this(NAME_PREFIX, values);
  }

  protected InSet(String name, Set<Object> values) {
    super(getName(name, values));
    this.values = getValues(values);
  }

  private static HashSet<Object> getValues(Set<Object> values) {
    HashSet<Object> result = new HashSet<>();
    for (Object value : values) {
      if (value instanceof byte[]) {
        result.add(new BinaryHolder((byte[]) value));
      } else if (value instanceof Number) {
        result.add(value);
      } else {
        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
      }
    }
    return result;
  }

  private static String getName(String name, Set<Object> values) {
    StringJoiner sj = new StringJoiner(",", name + "<", ">");
    for (Object value : values) {
      if (value instanceof byte[]) {
        sj.add(new String((byte[]) value));
      } else {
        sj.add(String.valueOf(value));
      }
    }
    return sj.toString();
  }

  @Override
  protected boolean evaluate(boolean input) {
    return values.contains(input);
  }

  @Override
  protected boolean evaluate(int input) {
    return values.contains(input);
  }

  @Override
  protected boolean evaluate(long input) {
    return values.contains(input);
  }

  @Override
  protected boolean evaluate(float input) {
    return values.contains(input);
  }

  @Override
  protected boolean evaluate(double input) {
    return values.contains(input);
  }

  @Override
  protected boolean evaluate(ArrowBufPointer input) {
    byte[] bytes = new byte[(int) input.getLength()];
    ArrowBuf buf = input.getBuf();
    if (buf != null) {
      buf.getBytes(input.getOffset(), bytes);
    }
    return values.contains(new BinaryHolder(bytes));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InSet inSet = (InSet) o;
    return Objects.equals(values, inSet.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), values);
  }
}
