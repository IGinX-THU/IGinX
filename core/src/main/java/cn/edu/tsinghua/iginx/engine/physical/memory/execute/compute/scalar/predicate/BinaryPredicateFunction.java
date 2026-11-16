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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.BinaryScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public abstract class BinaryPredicateFunction extends BinaryScalarFunction<BitVector>
    implements PredicateFunction {

  protected BinaryPredicateFunction(String name) {
    super(name);
  }

  @Nullable
  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    if (input.getSchema().getFields().size() != 2) {
      throw new ComputeException(getName() + " requires two arguments");
    }
    return filter(allocator, dictionaryProvider, input.getVector(0), input.getVector(1), selection);
  }

  @Nullable
  protected abstract BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      FieldVector left,
      FieldVector right,
      @Nullable BaseIntVector selection)
      throws ComputeException;
}
