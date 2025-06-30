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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArrowDictionaries;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public abstract class AbstractScalarExpression<OUTPUT extends FieldVector>
    implements ScalarExpression<OUTPUT> {

  private final List<ScalarExpression<?>> children;
  private final String alias;

  protected AbstractScalarExpression(String alias, List<? extends ScalarExpression<?>> children) {
    this.alias = alias;
    this.children = Collections.unmodifiableList(children);
  }

  @Override
  public List<ScalarExpression<?>> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return getName() + (alias == null ? "" : " AS " + alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), alias, children);
  }

  public abstract boolean equals(Object obj);

  @Override
  public OUTPUT invoke(BufferAllocator allocator, VectorSchemaRoot input) throws ComputeException {
    return invoke(allocator, null, input);
  }

  @Override
  public OUTPUT invoke(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    return invoke(allocator, ArrowDictionaries.emptyProvider(), selection, input);
  }

  @Override
  public OUTPUT invoke(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException {
    if (alias == null) {
      return invokeImpl(allocator, dictionaryProvider, selection, input);
    } else {
      try (OUTPUT result = invokeImpl(allocator, dictionaryProvider, selection, input)) {
        return ValueVectors.transfer(allocator, result, alias);
      }
    }
  }

  protected abstract OUTPUT invokeImpl(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException;
}
