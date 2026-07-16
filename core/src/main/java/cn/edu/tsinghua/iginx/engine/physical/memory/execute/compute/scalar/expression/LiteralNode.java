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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantPool;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public class LiteralNode<OUTPUT extends FieldVector> extends AbstractScalarExpression<OUTPUT> {

  protected final ConstantPool pool;
  protected final Object value;

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
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    LiteralNode<?> literalNode = (LiteralNode<?>) obj;
    return Objects.deepEquals(value, literalNode.value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public OUTPUT invokeImpl(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException {
    int count = selection == null ? input.getRowCount() : selection.getValueCount();
    return (OUTPUT) ConstantVectors.of(allocator, pool, value, count);
  }

  @Override
  public Set<ScalarExpression<?>> getLeafExpressions() {
    return Collections.singleton(this);
  }
}
