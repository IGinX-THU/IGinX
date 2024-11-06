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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.UnaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class UnaryPredicateFunction extends UnaryScalarFunction<BitVector>
    implements PredicateFunction {

  protected UnaryPredicateFunction(String name) {
    super(name);
  }

  @Nullable
  @Override
  public BaseIntVector filter(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (input.getSchema().getFields().size() != 1) {
      throw new ComputeException(getName() + " requires one argument");
    }
    return filter(allocator, selection, input.getVector(0));
  }

  @Nullable
  protected abstract BaseIntVector filter(
      BufferAllocator allocator, @Nullable BaseIntVector selection, FieldVector input)
      throws ComputeException;
}
