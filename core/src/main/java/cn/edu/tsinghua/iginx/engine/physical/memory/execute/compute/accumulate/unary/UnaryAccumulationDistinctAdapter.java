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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.Schemas;
import java.util.HashSet;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;

public class UnaryAccumulationDistinctAdapter extends UnaryAccumulation {

  private final UnaryAccumulation delegate;

  public UnaryAccumulationDistinctAdapter(UnaryAccumulation delegate) {
    super(delegate.getName() + "_distinct");
    this.delegate = delegate;
  }

  @Override
  protected UnaryAccumulator<DistinctUnaryState> accumulate(
      BufferAllocator allocator, Field inputField) throws ComputeException {
    UnaryAccumulator<?> accumulator = delegate.accumulate(allocator, inputField);
    Field outputField =
        Schemas.fieldWithName(
            inputField, delegate.getName() + "(distinct " + inputField.getName() + ")");
    return new DistinctUnaryAccumulator(allocator, inputField, outputField, accumulator);
  }

  protected class DistinctUnaryAccumulator extends UnaryAccumulator<DistinctUnaryState> {

    protected DistinctUnaryAccumulator(
        BufferAllocator allocator,
        Field inputField,
        Field outputField,
        UnaryAccumulator<?> accumulator) {
      super(
          allocator,
          inputField,
          outputField,
          DistinctUnaryState.class,
          () -> new DistinctUnaryState(allocator, accumulator.createState()));
    }
  }

  protected static class DistinctUnaryState extends UnaryState {

    private final BufferAllocator allocator;
    private final UnaryState delegate;
    private final HashSet<Object> valueSet = new HashSet<>();

    protected DistinctUnaryState(BufferAllocator allocator, UnaryState delegate) {
      this.allocator = Objects.requireNonNull(allocator);
      this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void evaluate(FieldWriter writer) throws ComputeException {
      delegate.evaluate(writer);
    }

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      try (FieldVector distinct = inputVector.getField().createVector(allocator)) {
        distinct.setInitialCapacity(inputVector.getValueCount());
        int distinctCount = 0;
        for (int row = 0; row < inputVector.getValueCount(); row++) {
          Object value = inputVector.getObject(row);
          if (valueSet.contains(value)) {
            continue;
          }
          valueSet.add(value);
          distinct.copyFromSafe(row, distinctCount, inputVector);
          distinctCount++;
        }
        if (distinctCount > 0) {
          distinct.setValueCount(distinctCount);
          delegate.update(distinct);
        }
      }
    }
  }
}
