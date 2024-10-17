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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class BinaryFunction extends AbstractFunction {

  protected BinaryFunction(String name) {
    super(name, Arity.BINARY);
  }

  @Override
  protected FieldVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    return evaluate(allocator, input.getFieldVectors().get(0), input.getFieldVectors().get(1));
  }

  public abstract FieldVector evaluate(
      @WillNotClose BufferAllocator allocator,
      @WillNotClose FieldVector left,
      @WillNotClose FieldVector right)
      throws ComputeException;
}
