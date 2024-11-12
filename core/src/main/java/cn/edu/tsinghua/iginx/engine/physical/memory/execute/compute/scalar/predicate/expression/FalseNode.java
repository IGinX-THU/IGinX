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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.SelectionBuilder;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ConstantPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import javax.annotation.Nullable;

public class FalseNode extends LiteralNode<BitVector> implements PredicateExpression {

  public FalseNode(ConstantPool pool, String alias) {
    super(false, pool, alias);
  }

  public FalseNode(ConstantPool pool) {
    this(pool, null);
  }

  @Nullable
  @Override
  public BaseIntVector filter(BufferAllocator allocator, DictionaryProvider dictionaryProvider, VectorSchemaRoot input, @Nullable BaseIntVector selection) throws ComputeException {
    try (SelectionBuilder builder = new SelectionBuilder(allocator, "false", 0)) {
      return builder.build();
    }
  }
}
