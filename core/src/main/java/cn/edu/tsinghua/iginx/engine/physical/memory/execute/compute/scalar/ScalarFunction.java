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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface ScalarFunction extends PhysicalFunction {

  /**
   * Invoke the function with the given arguments.
   *
   * @param allocator the allocator to allocate memory
   * @param input the input vector schema root
   * @return the result of the function, may be a struct vector
   * @throws ComputeException if the function cannot be executed
   */
  FieldVector invoke(@WillNotClose BufferAllocator allocator, @WillNotClose VectorSchemaRoot input)
      throws ComputeException;
}

// TODO: give name to the function return
