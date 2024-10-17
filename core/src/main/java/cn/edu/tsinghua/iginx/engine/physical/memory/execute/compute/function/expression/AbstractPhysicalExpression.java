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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.Collections;
import java.util.List;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class AbstractPhysicalExpression implements PhysicalExpression {

  private final List<PhysicalExpression> children;
  private final String alias;

  protected AbstractPhysicalExpression(String alias, List<PhysicalExpression> children) {
    this.alias = alias;
    this.children = Collections.unmodifiableList(children);
  }

  @Override
  public List<PhysicalExpression> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return getName() + (alias == null ? "" : " as " + alias);
  }

  @Override
  public FieldVector invoke(
      @WillNotClose BufferAllocator allocator, @WillNotClose VectorSchemaRoot input)
      throws ComputeException {
    if (alias == null) {
      return invokeImpl(allocator, input);
    } else {
      try (FieldVector result = invokeImpl(allocator, input)) {
        return ValueVectors.transfer(allocator, result, alias);
      }
    }
  }

  protected abstract FieldVector invokeImpl(
      @WillNotClose BufferAllocator allocator, @WillNotClose VectorSchemaRoot input)
      throws ComputeException;
}
