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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArrowDictionaries;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public interface ScalarFunction<OUTPUT extends FieldVector> extends PhysicalFunction {

  /**
   * Invoke the function with the given arguments.
   *
   * @param allocator the allocator to allocate memory
   * @param input the input vector schema root
   * @return the result of the function, may be a struct vector
   * @throws ComputeException if the function cannot be executed
   */
  OUTPUT invoke(BufferAllocator allocator, VectorSchemaRoot input) throws ComputeException;

  /**
   * Invoke the function with the given selected arguments.
   *
   * @param allocator the allocator to allocate memory
   * @param selection the selection vector
   * @param input the input vector schema root
   * @return the result of the function, may be a struct vector
   * @throws ComputeException if the function cannot be executed
   */
  default OUTPUT invoke(BufferAllocator allocator, BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    try (VectorSchemaRoot selectedInput = PhysicalFunctions.take(allocator, selection, input)) {
      return invoke(allocator, selectedInput);
    }
  }

  // TODO: implement the invoke method rather than just use the default one
  default OUTPUT invoke(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException {
    if (dictionaryProvider.getDictionaryIds().isEmpty()) {
      return invoke(allocator, selection, input);
    }
    try (VectorSchemaRoot flattened =
        ArrowDictionaries.flatten(allocator, dictionaryProvider, input, selection)) {
      return invoke(allocator, flattened);
    }
  }
}

// TODO: give name to the function return
