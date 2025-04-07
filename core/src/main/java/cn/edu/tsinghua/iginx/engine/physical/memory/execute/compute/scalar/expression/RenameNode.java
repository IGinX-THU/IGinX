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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public class RenameNode<OUTPUT extends FieldVector> extends AbstractScalarExpression<OUTPUT> {

  private final ScalarExpression<OUTPUT> child;

  public RenameNode(String alias, ScalarExpression<OUTPUT> child) {
    super(Objects.requireNonNull(alias), Collections.singletonList(child));
    this.child = Objects.requireNonNull(child);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RenameNode<?> that = (RenameNode<?>) o;
    return Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), child);
  }

  @Override
  protected OUTPUT invokeImpl(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      @Nullable BaseIntVector selection,
      VectorSchemaRoot input)
      throws ComputeException {
    return child.invoke(allocator, dictionaryProvider, selection, input);
  }

  @Override
  public String getName() {
    return "(" + child.getName() + ")";
  }

  @Override
  public Set<ScalarExpression<?>> getLeafExpressions() {
    return child.getLeafExpressions();
  }
}
