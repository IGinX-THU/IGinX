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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.Accumulator;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ValueVectors;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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

    private final BufferAllocator allocator;
    private final UnaryAccumulator<?> accumulator;

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
      this.allocator = Objects.requireNonNull(allocator);
      this.accumulator = Objects.requireNonNull(accumulator);
    }

    @Override
    public FieldVector evaluate(List<State> states) throws ComputeException {
      List<Accumulator.State> delegateStates =
          states.stream()
              .map(DistinctUnaryState.class::cast)
              .map(DistinctUnaryState::getDelegate)
              .collect(Collectors.toList());
      try (FieldVector result = accumulator.evaluate(delegateStates)) {
        return ValueVectors.transfer(allocator, result, outputField.getName());
      }
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

    public UnaryState getDelegate() {
      return delegate;
    }

    @Override
    public void update(FieldVector inputVector) throws ComputeException {
      try (FieldVector distinct = inputVector.getField().createVector(allocator)) {
        distinct.setInitialCapacity(inputVector.getValueCount());
        int distinctCount = 0;
        for (int row = 0; row < inputVector.getValueCount(); row++) {
          Object value = inputVector.getObject(row);
          if (value instanceof byte[]) {
            value = new Binary((byte[]) value);
          }
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

    private static class Binary {
      private final byte[] value;

      public Binary(byte[] value) {
        this.value = value;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
          return false;
        }
        Binary binary = (Binary) obj;
        return Arrays.equals(value, binary.value);
      }

      @Override
      public int hashCode() {
        return Arrays.hashCode(value);
      }
    }
  }
}
