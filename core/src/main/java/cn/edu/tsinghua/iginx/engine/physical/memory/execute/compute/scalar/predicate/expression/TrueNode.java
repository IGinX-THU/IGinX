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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantPool;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public class TrueNode extends LiteralNode<BitVector> implements PredicateExpression {

  public TrueNode(ConstantPool pool, String alias) {
    super(true, pool, alias);
  }

  public TrueNode(ConstantPool pool) {
    this(pool, null);
  }

  @Nullable
  @Override
  public BaseIntVector filter(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot input,
      @Nullable BaseIntVector selection)
      throws ComputeException {
    return ValueVectors.slice(allocator, selection);
  }

  @Override
  public String getName() {
    return "true";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TrueNode falseNode = (TrueNode) o;
    return Objects.equals(value, falseNode.value) && Objects.equals(pool, falseNode.pool);
  }
}
