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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArityException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import java.util.Objects;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class AbstractAccumulator implements Accumulator {

  private final String name;
  private final Arity arity;

  protected AbstractAccumulator(String name, Arity arity) {
    this.name = Objects.requireNonNull(name);
    this.arity = Objects.requireNonNull(arity);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public State initialize(@WillNotClose BufferAllocator allocator, @WillNotClose Schema schema)
      throws ComputeException {
    if (!arity.checkArity(schema.getFields().size())) {
      throw new ArityException(this, arity, schema.getFields().size());
    }
    return initializeImpl(allocator, schema);
  }

  protected abstract AbstractState initializeImpl(
      @WillNotClose BufferAllocator allocator, @WillNotClose Schema schema) throws ComputeException;

  protected abstract static class AbstractState implements State {

    protected final BufferAllocator allocator;
    protected final Schema schema;
    protected final AbstractAccumulator accumulator;

    protected AbstractState(
        BufferAllocator allocator, Schema schema, AbstractAccumulator accumulator) {
      this.allocator = allocator;
      this.schema = schema;
      this.accumulator = accumulator;
    }

    @Override
    public void accumulate(VectorSchemaRoot root) throws ComputeException {
      if (!root.getSchema().equals(schema)) {
        throw new ComputeException(
            "Schema mismatch: expected " + schema + ", got " + root.getSchema());
      }
      accumulateImpl(root);
    }

    protected abstract void accumulateImpl(VectorSchemaRoot root) throws ComputeException;
  }
}
