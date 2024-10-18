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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FieldNode extends AbstractPhysicalExpression {

  private final int index;

  public FieldNode(int index) {
    this(index, null);
  }

  public FieldNode(int index, String alias) {
    super(alias, Collections.emptyList());
    this.index = index;
  }

  @Override
  public String getName() {
    return "field(" + index + ")";
  }

  @Override
  protected FieldVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    List<FieldVector> args = input.getFieldVectors();
    if (index >= args.size() || index < 0) {
      throw new ComputeException(
          "Field index out of bound, index: " + index + ", size: " + args.size());
    }
    return ValueVectors.slice(allocator, args.get(index), input.getRowCount());
  }
}

// TODO: optimize this node with Run-End-Encoding (REE) in Arrow-18.0.0, which requires java 11.
