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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ArityException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class AbstractScalarFunction<OUTPUT extends FieldVector>
    implements ScalarFunction<OUTPUT> {

  private final String name;
  private final Arity arity;

  protected AbstractScalarFunction(String name, Arity arity) {
    this.name = Objects.requireNonNull(name);
    this.arity = Objects.requireNonNull(arity);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public OUTPUT invoke(BufferAllocator allocator, VectorSchemaRoot input) throws ComputeException {
    return invokeImpl(allocator, null, input);
  }

  @Override
  public OUTPUT invoke(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    Schema schema = input.getSchema();
    if (!arity.checkArity(schema.getFields().size())) {
      throw new ArityException(this, schema, arity);
    }
    return invokeImpl(allocator, selection, input);
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arity);
  }

  public abstract boolean equals(Object obj);

  protected abstract OUTPUT invokeImpl(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException;
}
