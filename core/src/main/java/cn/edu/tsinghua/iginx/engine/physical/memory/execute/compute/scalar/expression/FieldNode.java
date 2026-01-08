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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public final class FieldNode extends AbstractScalarExpression<FieldVector> {

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

  public int getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FieldNode that = (FieldNode) obj;
    return index == that.index;
  }

  @Override
  protected FieldVector invokeImpl(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException {
    List<FieldVector> args = input.getFieldVectors();
    if (index >= args.size() || index < 0) {
      throw new ComputeException(
          "Field index out of bound, index: " + index + ", size: " + args.size());
    }
    if (selection == null) {
      return ValueVectors.slice(allocator, args.get(index), input.getRowCount());
    }
    return PhysicalFunctions.take(allocator, selection, args.get(index));
  }

  @Override
  public Set<ScalarExpression<?>> getLeafExpressions() {
    return Collections.singleton(this);
  }
}

// TODO: optimize this node with Run-End-Encoding (REE) in Arrow-18.0.0, which requires java 11.
