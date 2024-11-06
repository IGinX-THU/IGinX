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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantPool;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import java.util.Collections;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class LiteralNode<OUTPUT extends FieldVector> extends AbstractScalarExpression<OUTPUT> {

  private final ConstantPool pool;
  private final Object value;

  public LiteralNode(@Nullable Object value, ConstantPool pool) {
    this(value, pool, null);
  }

  public LiteralNode(@Nullable Object value, ConstantPool pool, String alias) {
    super(alias, Collections.emptyList());
    this.value = value;
    this.pool = pool;
  }

  @Override
  public String getName() {
    if (value == null) {
      return "null";
    }
    if (value instanceof byte[]) {
      return "'" + new String((byte[]) value) + "'";
    }
    return new Value(value).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LiteralNode) {
      LiteralNode<?> literalNode = (LiteralNode<?>) obj;
      return Objects.deepEquals(value, literalNode.value);
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected OUTPUT invokeImpl(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (selection == null) {
      return (OUTPUT) ConstantVectors.of(allocator, pool, value, input.getRowCount());
    }
    return (OUTPUT) ConstantVectors.of(allocator, pool, value, selection.getValueCount());
  }
}
